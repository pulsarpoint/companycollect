import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
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


class LookupBatchApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_four_workers_and_no_publication_before_whole_batch_finishes(self):
        await self.exercise_batch()

    async def test_basic_and_full_batches_preserve_submission_metadata(self):
        for crawl_type in ("site_info", "full"):
            await self.exercise_batch(crawl_type)

    async def exercise_batch(self, crawl_type=None):
        with TemporaryDirectory() as directory:
            service = CrawlService(
                Path(directory),
                {
                    "CLICKHOUSE_URL": "http://registry",
                    "CLICKHOUSE_RESULTS_URL": "http://results",
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
                output = result(job.domain, failed=job.domain == "a.se")
                write_json(attempt / "result.json", output)
                active -= 1
                return output

            async def transport(_transport, request):
                self.assertEqual(request.url.host, "results")
                inserts.append(
                    (
                        request.url.params["query"],
                        [
                            json.loads(line)
                            for line in request.content.decode().split("\n")
                        ],
                    )
                )
                return httpx.Response(200)

            async def until(predicate):
                async with asyncio.timeout(12):
                    while not predicate():
                        await asyncio.sleep(0.05)

            with (
                patch.object(service, "run_scan", scan),
                patch.object(
                    httpx.AsyncHTTPTransport, "handle_async_request", transport
                ),
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
                        domains=["a.se", "b.se", "c.se", "d.se", "e.se"],
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
                "CLICKHOUSE_RESULTS_URL": "http://results",
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

            async def transport(_transport, request):
                return httpx.Response(200)

            with patch.object(
                httpx.AsyncHTTPTransport, "handle_async_request", transport
            ):
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

        def respond(request):
            query = request.url.params["query"]
            attempts.append(query)
            return httpx.Response(503 if fail else 200)

        async with httpx.AsyncClient(
            base_url="http://results", transport=httpx.MockTransport(respond)
        ) as http:
            with self.assertRaises(httpx.HTTPStatusError):
                await publish(http, [result()])
            self.assertEqual(len(attempts), 1)
            fail = False
            await publish(http, [result()])
        self.assertIn("website_company_lookup_results", attempts[-1])


class ClickHousePublicationTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_schema_replay_and_latest_proposal(self):
        import os

        endpoint = os.getenv("LOOKUP_CLICKHOUSE_TEST_URL")
        if not endpoint:
            self.skipTest("Set LOOKUP_CLICKHOUSE_TEST_URL to an isolated test database")
        async with httpx.AsyncClient(base_url=endpoint, timeout=30) as http:
            migrations = (
                Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
            )
            for name in (
                "000429_corpscout_website_crawl_requests",
                "000430_corpscout_website_crawl_type_results",
                "000459_corpscout_website_company_lookup_results",
            ):
                # These two migrations contain no procedural SQL or semicolons in literals.
                for statement in (
                    (migrations / (name + ".up.sql")).read_text().split(";")
                ):
                    if statement.strip():
                        response = await http.post("", content=statement.encode())
                        self.assertEqual(response.status_code, 200, response.text)
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
            await publish(http, [matched])
            await publish(http, [matched])
            for table, rows in result_rows(matched).items():
                if not rows:
                    continue
                response = await http.post(
                    "",
                    content=f"SELECT count() FROM corpscout.{table} FINAL WHERE domain='integration.se'".encode(),
                )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.text.strip(), "1", table)
            latest = result("integration.se")
            latest["request_id"] = "new-lookup"
            await publish(http, [latest])
            response = await http.post(
                "",
                content=b"SELECT count() FROM corpscout.website_company_lookup_proposals WHERE domain='integration.se'",
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.text.strip(), "0")

            # Full collection success survives a matching failure, with the same request identity.
            full = result("full.se", failed=True)
            full.update(crawl_type="full", request_id="full-crawl", input_revision=9,
                crawl_result=full["site_info_result"])
            await publish(http, [full])
            await publish(http, [full])
            response = await http.post("", content=b"SELECT request_id, input_revision, successful, company_matching_status FROM corpscout.website_full_crawl_results FINAL WHERE domain='full.se' FORMAT JSONEachRow")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(json.loads(response.text), dict(request_id="full-crawl", input_revision=9, successful=True, company_matching_status="failed"))
            # Match prechecks read active, country-scoped associations, including review overrides in the production view.
            await http.post("", content=b"CREATE TABLE corpscout.company_domains_resolved (country_code String, root_domain String, company_id String, is_active Bool) ENGINE=Memory")
            response = await http.post("", content=b"INSERT INTO corpscout.company_domains_resolved VALUES ('SE', 'mapped.se', '5560123456', true), ('SE', 'mapped.se', '5560999999', false), ('NO', 'mapped.se', '5560888888', true)")
            self.assertEqual(response.status_code, 200, response.text)
            from crawler_service.company_search import search_companies
            searches = []
            mapped = await search_companies(http, kind="existing_mapping", value="www.mapped.se", searches=searches)
            self.assertEqual(mapped, [{"company_id": "5560123456"}])
            self.assertEqual(searches[0]["row_count"], 1)
