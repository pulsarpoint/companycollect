import asyncio
import json
import os
import re
import sqlite3
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

import httpx
from clickhouse_driver import Client
from corpscout_identity.registration import identify_website
from corpscout_identity.urls import website_reference
from test_llm_profile import KEY, profile_payload

from crawler_service.company_lookup import CompanyLookupBatchRequest
from crawler_service.company_lookup_results import publish, result_rows
from crawler_service.company_lookup_store import LookupStore
from crawler_service.profiles import site_information
from crawler_service.service import CrawlService
from crawler_service.service_api import create_app
from crawler_service.storage import utc_now, write_json


def result(domain="example.se", *, failed=False):
    now = utc_now()
    info = site_information(None, f"https://{domain}/") | {
        "site_description": "Example provides engineering services.",
        "evidence_status": "source_matched",
        "site_types": ["company"],
    }
    return dict(
        schema_version="website-company-lookup/1.1",
        country="SE",
        domain=domain,
        website_url=f"https://{domain}/",
        website_id=website_reference(f"https://{domain}/"),
        request_id="lookup-test",
        attempt=1,
        work_key="settings",
        status="failed" if failed else "not_found",
        found=False,
        started_at=now,
        finished_at=now,
        stop_reason="registry_unavailable" if failed else "no_candidates",
        reasons=[],
        result_path="jobs/lookup-test/attempts/0001/result.json",
        site_info=info,
        site_info_result={
            "schema_version": "company-crawl-result/1.2",
            "documents": [],
            "crawl": dict(
                status="finished",
                stop_reason="site_info_complete",
                site_info=info,
                pages=[],
                usage={},
                started_at=now,
                finished_at=now,
            ),
        },
    )


class LookupStoreTests(unittest.TestCase):
    def test_legacy_queue_migration_preserves_request_and_batch_state(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite3"
            old = sqlite3.connect(path)
            old.executescript("""CREATE TABLE items (
                request_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, domain TEXT NOT NULL,
                dispatched INTEGER NOT NULL DEFAULT 0, cancelled INTEGER NOT NULL DEFAULT 0);
                INSERT INTO items VALUES ('a','old','a.se',1,0),('b','old','b.se',0,1);""")
            old.close()
            for _ in range(2):
                store = LookupStore(path)
                self.assertEqual([tuple(row) for row in store.db.execute("SELECT * FROM items ORDER BY request_id")],
                                 [("a", "old", "a.se", 1, 0), ("b", "old", "b.se", 0, 1)])
                store.db.execute("INSERT OR IGNORE INTO items VALUES ('b','new','b.se',0,0)")
                self.assertEqual(store.db.execute("SELECT count(*) FROM items").fetchone()[0], 3)
                store.db.rollback()
                store.close()

    def test_partial_cancelled_batch_can_resume_with_new_membership_after_restart(self):
        def payload(batch, domains):
            return {"batch_id": batch, "input_id": "task", "run_id": "execution", "entries": [
                {"request": {"request_id": domain, "url": f"https://{domain}/",
                             "llm": {"profile_id": "model", "profile_revision": 1,
                                     "api_key_encrypted": batch}}, "work_key": domain}
                for domain in domains]}

        with TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite3"
            store = LookupStore(path)
            store.submit("old", payload("old", ["a.se", "b.se"]), [
                {"request_id": domain, "domain": domain} for domain in ["a.se", "b.se"]])
            store.dispatched("a.se")
            store.enqueue("a.se", 1, result("a.se"))
            store.cancel("old")
            batch, rows = store.ready()[0]
            store.delivered(batch, rows, None)
            store.close()
            store = LookupStore(path)
            store.submit("new", payload("new", ["b.se", "c.se"]), [
                {"request_id": domain, "domain": domain} for domain in ["b.se", "c.se"]])
            self.assertEqual(store.snapshot("old")["total"], 2)
            self.assertEqual([r["domain"] for r in store.undispatched(4)], ["b.se", "c.se"])
            for domain in ["b.se", "c.se"]:
                store.dispatched(domain)
                store.enqueue(domain, 1, result(domain))
            self.assertEqual(store.undispatched(4), [])
            batch, rows = store.ready()[0]
            self.assertEqual(batch, "new")
            self.assertEqual(len(rows), 2)
            store.delivered(batch, rows, None)
            self.assertEqual(store.snapshot("old")["state"], "cancelled")
            self.assertEqual(store.snapshot("new")["state"], "published")
            self.assertEqual(store.snapshot("new")["processed"], 2)
            self.assertEqual(store.ready(), [])
            with self.assertRaisesRegex(ValueError, "Request ID already exists"):
                store.submit("conflict", payload("conflict", ["b.se"]) | {"run_id": "different-execution"},
                             [{"request_id": "b.se", "domain": "b.se"}])
            self.assertIsNone(store.snapshot("conflict"))
            store.close()

    def test_overlapping_active_batches_dispatch_once_and_cancel_only_exclusive_work(self):
        with TemporaryDirectory() as directory:
            store = LookupStore(Path(directory) / "queue.sqlite3")
            for batch in ["one", "two"]:
                store.submit(batch, {"domains": ["a.se"]}, [{"request_id": "a", "domain": "a.se"}])
            self.assertEqual(len(store.undispatched(4)), 1)
            store.dispatched("a")
            self.assertEqual(store.undispatched(4), [])
            self.assertEqual(store.cancel("one"), [])
            self.assertEqual(store.cancel("two"), ["a"])
            store.close()

    def test_restart_holds_partial_batch_and_replays_only_unacknowledged_rows(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite3"
            store = LookupStore(path)
            store.submit(
                "batch",
                {"domains": ["a.se", "b.se"]},
                [
                    {"request_id": "a", "domain": "a.se"},
                    {"request_id": "b", "domain": "b.se"},
                ],
            )
            store.enqueue("a", 1, result("a.se"))
            self.assertEqual(store.ready(), [])
            store.close()
            store = LookupStore(path)
            store.enqueue("b", 1, result("b.se", failed=True))
            batch, rows = store.ready()[0]
            self.assertEqual(store.snapshot(batch)["processed"], 2)
            store.delivered(batch, rows, "HTTPStatusError")
            self.assertEqual(store.snapshot(batch)["state"], "publishing")
            self.assertEqual(len(store.ready()[0][1]), 2)
            store.delivered(batch, rows, None)
            self.assertEqual(store.snapshot(batch)["state"], "published")
            self.assertEqual(store.ready(), [])
            store.enqueue("a", 1, result("a.se", failed=True))
            self.assertEqual(store.ready(), [])
            with self.assertRaises(ValueError):
                store.submit("batch", {"domains": ["other.se"]}, [])
            store.close()

    def test_same_saved_revision_keeps_original_ciphertext_on_re_admission(self):
        with TemporaryDirectory() as directory:
            store = LookupStore(Path(directory) / "queue.sqlite3")
            first = {
                "llm": {
                    "profile_id": "profile",
                    "profile_revision": 1,
                    "api_key_encrypted": "cipher-1",
                }
            }
            store.submit("batch", first, [{"request_id": "site", "domain": "site.se"}])
            second = {"llm": first["llm"] | {"api_key_encrypted": "cipher-2"}}
            store.submit("batch", second, [])
            self.assertEqual(json.loads(store.undispatched(1)[0]["payload"]), first)
            with self.assertRaises(ValueError):
                store.submit(
                    "batch", {"llm": second["llm"] | {"profile_revision": 2}}, []
                )
            store.close()

    def test_good_basic_result_survives_failed_company_matching(self):
        rows = result_rows(result(failed=True))
        basic = rows["website_site_info_results"][0]
        self.assertTrue(basic["successful"])
        self.assertEqual(basic["state"], "completed")
        self.assertEqual(basic["error"], "")
        self.assertEqual(rows["website_company_lookup_results"][0]["status"], "failed")


def parent_rows(query, params, **kwargs):
    if not query.startswith("SELECT p.page_id"):
        return None
    ids = params["ids"]
    domains = ["alpha.se", "beta.se", "gamma.se", "delta.se", "epsilon.se", "example.se"] + [f"site-{i}.se" for i in range(6)]
    identities = [identify_website(f"https://{domain}/") for domain in domains]
    return [(item.page_id, item.website_id, item.domain_id, item.page_url)
            for item in identities if item.page_id in ids]


class LookupBatchApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # These tests exercise the HTTP/SQLite scheduling boundary. The real parent
        # registration + publication boundary is covered by identity integration tests.
        for target in ("crawler_service.service_api.register_requests", "crawler_service.service.register_results"):
            mocked = self.enterContext(patch(target))
            if target.endswith("register_results"):
                self.registration = mocked
        self.native_factory = self.enterContext(
            patch("crawler_service.company_lookup_results.Client.from_url")
        )
        self.native = self.native_factory.return_value
        self.native.execute.side_effect = parent_rows

    async def test_four_workers_and_no_publication_before_whole_batch_finishes(self):
        await self.exercise_batch()

    async def test_basic_and_full_batches_preserve_submission_metadata(self):
        for crawl_type in ("site_info", "full"):
            await self.exercise_batch(crawl_type)

    async def test_registration_outage_keeps_completed_payloads_without_recrawling(self):
        self.registration.side_effect = ValueError("identity database unavailable")
        await self.exercise_batch(registration_outage=True)

    async def exercise_batch(self, crawl_type=None, registration_outage=False):
        with TemporaryDirectory() as directory:
            service = CrawlService(
                Path(directory),
                {
                    "CLICKHOUSE_URL": "http://registry",
                    "CLICKHOUSE_RESULTS_NATIVE_URL": "clickhouse://writer:password@results:9000/corpscout",
                    "CRAWLER_LLM_ENCRYPTION_KEY": KEY,
                },
                concurrency=1,
                max_pending=20,
            )
            app = create_app(service, api_token="test-token")
            release, last_release = asyncio.Event(), asyncio.Event()
            active, peak, started = 0, 0, []
            inserts = []

            async def scan(request, job, attempt, human):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                started.append(job.domain)
                await (release if len(started) <= 4 else last_release).wait()
                output = result(job.domain, failed=job.domain == "alpha.se")
                write_json(attempt / "result.json", output)
                active -= 1
                return output

            def execute(query, params, **kwargs):
                parents = parent_rows(query, params)
                if parents is not None:
                    return parents
                columns = query.split("(", 1)[1].split(")", 1)[0].split(",")
                inserts.append((query, [dict(zip(columns, row, strict=True)) for row in params]))
                return len(params)

            self.native.execute.side_effect = execute

            async def until(predicate):
                async with asyncio.timeout(12):
                    while not predicate():
                        await asyncio.sleep(0.05)

            with (
                patch.object(service, "run_scan", scan),
            ):
                async with (
                    app.router.lifespan_context(app),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app),
                        base_url="http://api",
                        headers={"Authorization": "Bearer test-token"},
                    ) as http,
                ):
                    payload = dict(
                        batch_id="batch",
                        domains=["alpha.se", "beta.se", "gamma.se", "delta.se", "epsilon.se"],
                        country="SE",
                        llm=profile_payload(),
                    )
                    endpoint = "/v1/company-lookup-batches"
                    if crawl_type is not None:
                        endpoint = "/v1/crawl-batches"
                        payload = dict(batch_id="batch", input_id="draft", run_id="execution", entries=[{
                            "request": {"request_id": f"crawl-{index}", "url": f"https://{domain}/", "llm": profile_payload(),
                                "site_info": True, "crawl": False if crawl_type == "site_info" else "full", "company_lookup": {"country": "SE", "skip_if_mapped": True}},
                            "crawl_type": crawl_type, "input_revision": 7, "work_key": "a" * 64,
                        } for index, domain in enumerate(payload["domains"])])
                    response = await http.post(
                        endpoint, json=payload
                    )
                    self.assertEqual(response.status_code, 202, response.text)
                    await until(lambda: len(started) == 4)
                    self.assertEqual(peak, 4)
                    self.assertEqual(inserts, [])
                    release.set()
                    await until(
                        lambda: (
                            len(started) == 5
                            and service.lookup_store.snapshot("batch")["processed"] == 4
                        )
                    )
                    self.assertEqual(service.lookup_store.ready(), [])
                    self.assertEqual(inserts, [])
                    last_release.set()
                    if registration_outage:
                        await until(lambda: service.lookup_store.snapshot("batch")["publication_error"])
                        self.assertEqual(len(started), 5)
                        self.assertEqual(inserts, [])
                        self.assertEqual(service.lookup_store.snapshot("batch")["processed"], 5)
                        self.assertEqual(len(service.lookup_store.ready()[0][1]), 5)
                        self.registration.side_effect = None
                    await until(
                        lambda: (
                            service.lookup_store.snapshot("batch")["state"]
                            == "published"
                        )
                    )
                    state = (await http.get("/v1/company-lookup-batches/batch")).json()
                    self.assertEqual((state["processed"], state["failed"]), (5, 1))
                    self.assertEqual(peak, 4)
                    basic = next(
                        rows
                        for query, rows in inserts
                        if "website_site_info_results" in query
                    )
                    self.assertEqual(len(basic), 5)
                    self.assertTrue(all(row["successful"] for row in basic))
                    if crawl_type is not None:
                        table = "website_site_info_results" if crawl_type == "site_info" else "website_full_crawl_results"
                        stored = next(rows for query, rows in inserts if table in query)
                        self.assertEqual(len(stored), 5)
                        self.assertTrue(all(row["work_key"] == "a" * 64 and row["input_revision"] == 7 and row["run_id"] == "execution" for row in stored))
                        self.assertEqual(sorted(row["request_id"] for row in stored), [f"crawl-{i}" for i in range(5)])
                    self.assertIn("website_company_lookup_results", inserts[-1][0])
                    self.assertEqual(
                        (await http.get("/v1/crawls/batch")).json()["state"],
                        "completed",
                    )
                    repeated = await http.post(
                        endpoint, json=payload
                    )
                    self.assertEqual(repeated.json()["state"], "published")
                    self.assertEqual(len(started), 5)
                    one = next(iter(service.jobs))
                    saved = (await http.get(f"/v1/crawls/{one}/result")).json()
                    self.assertTrue(saved["persisted_to_database"])

    async def test_restart_continues_unstarted_members_without_recrawling_saved_attempts(
        self,
    ):
        with TemporaryDirectory() as directory:
            environment = {
                "CLICKHOUSE_URL": "http://registry",
                "CLICKHOUSE_RESULTS_NATIVE_URL": "clickhouse://writer:password@results:9000/corpscout",
                "CRAWLER_LLM_ENCRYPTION_KEY": KEY,
            }
            service = CrawlService(
                Path(directory), environment, concurrency=1, max_pending=20
            )
            started = []

            async def blocked(request, job, attempt, human):
                started.append(job.request_id)
                await asyncio.Event().wait()

            service.run_scan = blocked

            with patch("crawler_service.company_lookup_results.Client.from_url", return_value=self.native):
                await service.start()
                service.submit_lookup_batch(
                    CompanyLookupBatchRequest(
                        batch_id="restart",
                        country="SE",
                        domains=[f"site-{i}.se" for i in range(6)],
                        llm=profile_payload(),
                    )
                )
                async with asyncio.timeout(5):
                    while len(started) < 4:
                        await asyncio.sleep(0.01)
                await service.close()
                resumed = CrawlService(
                    Path(directory), environment, concurrency=1, max_pending=20
                )
                new = []

                async def finish(request, job, attempt, human):
                    new.append(job.request_id)
                    output = result(job.domain)
                    write_json(attempt / "result.json", output)
                    return output

                resumed.run_scan = finish
                await resumed.start()
                try:
                    async with asyncio.timeout(12):
                        while (
                            resumed.lookup_store.snapshot("restart")["state"]
                            != "published"
                        ):
                            await asyncio.sleep(0.05)
                    state = resumed.lookup_store.snapshot("restart")
                    self.assertEqual((state["processed"], state["failed"]), (6, 4))
                    self.assertEqual(len(new), 2)
                    self.assertFalse(set(new) & set(started))
                finally:
                    await resumed.close()

    async def test_failure_before_browser_acquisition_is_still_publishable(self):
        with TemporaryDirectory() as directory:
            service = CrawlService(
                Path(directory),
                {
                    "CLICKHOUSE_URL": "http://registry",
                    "CRAWLER_LLM_ENCRYPTION_KEY": KEY,
                },
                concurrency=1,
                max_pending=10,
            )

            async def fail(*args):
                raise RuntimeError("No browser capacity")

            service.run_scan = fail
            app = create_app(service, api_token="token")
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app),
                    base_url="http://api",
                    headers={"Authorization": "Bearer token"},
                ) as http,
            ):
                submitted = await http.post(
                    "/v1/company-lookups",
                    json={
                        "request_id": "early-failure",
                        "domain": "example.se",
                        "country": "SE",
                        "llm": profile_payload(),
                    },
                )
                self.assertEqual(submitted.status_code, 202, submitted.text)
                job = await asyncio.wait_for(service.wait("early-failure"), 5)
                self.assertEqual(job.state, "failed")
                output = (await http.get("/v1/crawls/early-failure/result")).json()
                self.assertEqual(output["status"], "failed")
                self.assertEqual(output["candidates"], [])
                self.assertEqual(output["searches"], [])
                self.assertEqual(
                    output["site_info_result"]["crawl"]["status"], "failed"
                )
                queued = json.loads(service.lookup_store.ready()[0][1][0]["payload"])
                rows = result_rows(queued)
                self.assertFalse(rows["website_site_info_results"][0]["successful"])
                self.assertEqual(
                    rows["website_company_lookup_results"][0]["status"], "failed"
                )

    async def test_write_failure_does_not_send_summary_and_can_retry(self):
        attempts = []
        fail = True
        client = Mock(spec=Client)

        def execute(query, params, **kwargs):
            parents = parent_rows(query, params)
            if parents is not None:
                return parents
            attempts.append((query, kwargs["settings"]["insert_deduplication_token"]))
            if fail:
                raise ConnectionError("writer unavailable")
            return len(params)

        client.execute.side_effect = execute
        output = result()
        with self.assertRaisesRegex(RuntimeError, "ClickHouse insert failed for corpscout.website_site_info_results"):
            publish(client, [output])
        self.assertEqual(len(attempts), 1)
        fail = False
        publish(client, [output])
        self.assertEqual(attempts[0], attempts[1])
        self.assertIn("website_company_lookup_results", attempts[-1][0])


class ClickHousePublicationTests(unittest.TestCase):
    def setUp(self):
        endpoint = os.getenv("LOOKUP_CLICKHOUSE_TEST_NATIVE_URL")
        if not endpoint:
            self.skipTest("Set LOOKUP_CLICKHOUSE_TEST_NATIVE_URL to an isolated ClickHouse")
        self.client = Client.from_url(endpoint)
        self.addCleanup(self.client.disconnect)
        migrations = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
        for name in (
            "000429_corpscout_website_crawl_requests",
            "000430_corpscout_website_crawl_type_results",
            "000441_corpscout_domains_inventory",
            "000442_corpscout_websites_and_pages",
            "000459_corpscout_website_company_lookup_results",
        ):
            for statement in (migrations / (name + ".up.sql")).read_text().split(";"):
                if statement.strip():
                    self.client.execute(statement)
        # Apply only the relevant ALTERs from cross-service migrations.
        for name, tables in (
            ("000462_corpscout_domains_sources", ("domains", "websites", "pages")),
            ("000466_corpscout_domain_result_references", tuple(result_rows(result()))),
        ):
            source = re.sub(r"--[^\n]*", "", (migrations / (name + ".up.sql")).read_text())
            for statement in source.split(";"):
                if any(statement.strip().startswith(f"ALTER TABLE corpscout.{table}\n")
                       or statement.strip().startswith(f"ALTER TABLE corpscout.{table} ")
                       for table in tables):
                    self.client.execute(statement)
        self.client.execute("CREATE USER IF NOT EXISTS lookup_writer IDENTIFIED WITH plaintext_password BY 'test'")
        for table in ("domains", "websites", "pages"):
            self.client.execute(f"GRANT SELECT ON corpscout.{table} TO lookup_writer")
        for table in result_rows(result()):
            self.client.execute(f"GRANT INSERT ON corpscout.{table} TO lookup_writer")
        parsed = urlsplit(endpoint)
        writer_endpoint = parsed._replace(
            netloc=f"lookup_writer:test@{parsed.hostname}:{parsed.port or 9000}",
            path="/corpscout",
        ).geturl()
        self.writer = Client.from_url(writer_endpoint)
        self.addCleanup(self.writer.disconnect)

    def register(self, urls):
        now = datetime.now(UTC)
        for url in urls:
            identity = identify_website(url)
            self.client.execute(
                "INSERT INTO corpscout.domains (root_domain,sources,first_seen_at,last_seen_at,source_run_id) VALUES",
                [(identity.root_domain, ["test"], now, now, "test")],
            )
            self.client.execute(
                "INSERT INTO corpscout.websites (root_domain,website_origin,sources,first_seen_at,last_seen_at,source_run_id) VALUES",
                [(identity.root_domain, identity.website_origin, ["test"], now, now, "test")],
            )
            self.client.execute(
                "INSERT INTO corpscout.pages (root_domain,website_origin,page_url,sources,first_seen_at,last_seen_at,source_run_id) VALUES",
                [(identity.root_domain, identity.website_origin, identity.page_url, ["test"], now, now, "test")],
            )

    def test_real_schema_replay_and_latest_proposal(self):
        matched = result("integration.se")
        matched.update(
            status="matched",
            found=True,
            company_id="5560123456",
            confidence=0.98,
            candidates=[
                dict(
                    company_id="5560123456",
                    legal_name="Example AB",
                    status="active",
                    primary_city=None,
                )
            ],
            assessment=dict(
                company_id="5560123456",
                confidence=0.98,
                basis="registration_number",
                reasons=["Exact ID"],
            ),
            identity=[
                dict(
                    kind="registration_number",
                    value="556012-3456",
                    normalized_company_id="5560123456",
                    source_url="https://integration.se/",
                    quote="Org nr: 556012-3456",
                )
            ],
            searches=[
                dict(
                    query_id="query",
                    kind="registration_number",
                    table="corpscout.se_companies_serving",
                    sql="SELECT company_id WHERE company_id={value:String}",
                    parameters={"value": "5560123456"},
                    status="completed",
                    row_count=1,
                    rows=[{"company_id": "5560123456"}],
                )
            ],
        )
        with self.assertRaisesRegex(ValueError, "missing registered"):
            publish(self.writer, [matched])
        self.assertEqual(self.client.execute("SELECT count() FROM corpscout.website_company_lookup_results"), [(0,)])
        self.register([matched["website_url"], "https://full.se/"])
        publish(self.writer, [matched])
        publish(self.writer, [matched])
        for table, rows in result_rows(matched).items():
            if rows:
                self.assertEqual(self.client.execute(
                    f"SELECT count() FROM corpscout.{table} FINAL WHERE domain='integration.se'"
                ), [(1,)], table)
        # Native encoding preserves Map, nullable values, arrays, Enum, Bool and microseconds.
        self.assertEqual(self.client.execute(
            "SELECT parameters,http_status FROM corpscout.website_company_lookup_searches FINAL"
        ), [({"value": '"5560123456"'}, None)])
        self.assertEqual(self.client.execute(
            "SELECT primary_city,confidence,reasons FROM corpscout.website_company_lookup_candidates FINAL"
        ), [("", 0.98, ["Exact ID"])])
        saved = self.client.execute(
            "SELECT state,successful,finished_at FROM corpscout.website_site_info_results FINAL WHERE domain='integration.se'"
        )[0]
        self.assertEqual(saved[:2], ("completed", True))
        self.assertEqual(saved[2], datetime.fromisoformat(matched["finished_at"]))
        latest = result("integration.se")
        latest["request_id"] = "new-lookup"
        publish(self.writer, [latest])
        self.assertEqual(self.client.execute(
            "SELECT count() FROM corpscout.website_company_lookup_proposals WHERE domain='integration.se'"
        ), [(0,)])
        full = result("full.se", failed=True)
        full.update(crawl_type="full", request_id="full-crawl", input_revision=9,
                    crawl_result=full["site_info_result"])
        publish(self.writer, [full])
        publish(self.writer, [full])
        self.assertEqual(self.client.execute(
            "SELECT request_id,input_revision,successful,company_matching_status FROM corpscout.website_full_crawl_results FINAL WHERE domain='full.se'"
        ), [("full-crawl", 9, True, "failed")])


def test_legacy_batch_identity_enrichment_does_not_change_frozen_settings(tmp_path):
    store = LookupStore(tmp_path / "queue.sqlite3")
    old = {"batch_id": "old", "entries": [{"request": {"request_id": "a", "url": "https://a.se/"}}]}
    store.submit("old", old, [{"request_id": "a", "domain": "a.se"}])
    enriched = json.loads(json.dumps(old))
    enriched["entries"][0]["request"]["website_id"] = website_reference("https://a.se/")
    store.submit("old", enriched, [{"request_id": "a", "domain": "a.se"}])
    assert store.snapshot("old")["total"] == 1
    store.close()
