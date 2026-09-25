"""Result assets against real PostgreSQL/ClickHouse and the crawler HTTP boundary."""

import json
from contextlib import closing
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from uuid import uuid4

import dagster as dg
import psycopg2
import pytest

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.input import INPUT_TABLES
from dagster_v3.defs.website_crawl.results import (
    EXECUTION_TAG,
    RESULTS_BY_TYPE,
    SUBMISSIONS,
    CrawlResultsConfig,
)
from dagster_v3.defs.website_crawl.results_assets import (
    website_full_crawl_results,
    website_jobs_crawl_results,
    website_site_info_results,
)
from tests.test_processing_store import processing_postgres_url  # noqa: F401
from tests.test_website_crawl_input_assets import server  # noqa: F401

ASSETS = (
    website_full_crawl_results,
    website_jobs_crawl_results,
    website_site_info_results,
)


@pytest.fixture
def database(server, processing_postgres_url):  # noqa: F811
    client, resource = server
    migration = (
        Path(__file__).parents[3]
        / "clickhouse/migrations/000430_corpscout_website_crawl_type_results.up.sql"
    )
    for statement in migration.read_text().split(";"):
        if statement.strip():
            client.execute(statement)
    for table in (*INPUT_TABLES, *RESULTS_BY_TYPE.values(), SUBMISSIONS):
        client.execute(f"TRUNCATE TABLE {table}")
    return client, resource, ProcessingResource(postgres_url=processing_postgres_url)


@pytest.fixture
def crawler(monkeypatch):
    saved = {}
    calls = []
    behavior = {
        "pending": False,
        "reject": False,
        "partial": False,
        "s3_pending": False,
        # Request ids in the order the crawler created their job; an identical
        # re-POST returns the existing job and creates nothing.
        "created": [],
        # Answer the first status GET of each request 404 and drop its job,
        # like a crawler that lost its queue in a restart.
        "forget": False,
        # Terminal job state and result body overrides (e.g. a failed crawl).
        "state": None,
        "result": None,
        # Answer this many /result GETs 409 ("not ready") before serving it.
        "result_not_ready": 0,
    }
    forgotten = set()

    class Handler(BaseHTTPRequestHandler):
        def reply(self, code, body):
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_POST(self):
            assert self.headers["Authorization"] == "Bearer test-token"
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((self.path, body))
            if behavior["reject"]:
                return self.reply(422, {"detail": "invalid config"})
            if self.path == "/v1/crawls":
                known = saved.get(body["request_id"])
                if known is not None and known != body:
                    return self.reply(
                        409,
                        {"detail": "request_id already belongs to a different request"},
                    )
                if known is None:
                    saved[body["request_id"]] = body
                    behavior["created"].append(body["request_id"])
            self.reply(
                202 if self.path == "/v1/crawls" else 200,
                {"request_id": body["request_id"], "state": "queued"},
            )

        def do_GET(self):
            request_id = self.path.split("/")[3]
            if (
                behavior["forget"]
                and request_id in saved
                and request_id not in forgotten
                and not self.path.endswith("/result")
            ):
                forgotten.add(request_id)
                del saved[request_id]
            if request_id not in saved:
                return self.reply(404, {"detail": "Unknown crawl request"})
            payload = saved[request_id]
            now = datetime.now(UTC).isoformat()
            status = "partial" if behavior["partial"] else "finished"
            if self.path.endswith("/result"):
                if behavior["result_not_ready"]:
                    behavior["result_not_ready"] -= 1
                    return self.reply(409, {"detail": "Crawl result is not ready"})
                if behavior["result"] is not None:
                    return self.reply(200, behavior["result"])
                return self.reply(
                    200,
                    {
                        "crawl": {
                            "status": status,
                            "site_info": {
                                "description": "Makes sensors",
                                "services": ["Engineering"],
                            },
                            "pages": [
                                {"url": payload["url"], "fetch_status": "fetched"}
                            ],
                            "usage": {"calls": 1},
                        },
                        "documents": [
                            {
                                "html": "<h1>Large HTML stays in S3</h1>",
                                "input": {
                                    "observations": {
                                        "contacts": {"emails": ["hi@example.org"]}
                                    }
                                },
                            }
                        ],
                    },
                )
            self.reply(
                200,
                {
                    "request_id": request_id,
                    "attempt": 1,
                    "state": "running"
                    if behavior["pending"]
                    else behavior["state"] or "completed",
                    "started_at": now,
                    "finished_at": now,
                    "error": None,
                    "s3_state": "pending" if behavior["s3_pending"] else "uploaded",
                    "s3_event": {
                        "result": {
                            "bucket": "crawls",
                            "key": f"company-crawls/{request_id}/attempts/0001/result.json.gz",
                        }
                    },
                },
            )

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=http.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("CRAWLER_API_URL", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("CRAWLER_API_TOKEN", "test-token")
    yield saved, calls, behavior
    http.shutdown()
    http.server_close()
    thread.join()


def run(database, asset=website_site_info_results, *, instance=None, **config):
    _, resource, processing = database
    info = asset is website_site_info_results
    config = {
        "challenge_agent_model": "deepseek-flash",
        "challenge_agent_max_runs": 3,
        "api": "deepseek",
        "model": "deepseek-flash",
        "max_pages": 1 if info else 20,
        "max_model_calls": 20,
        "page_selection": "basic_info" if info else "saved",
        **config,
    }
    return dg.materialize(
        [
            asset,
            dg.AssetSpec("website_crawl_input"),
        ],
        resources={"clickhouse": resource, "processing": processing},
        run_config={
            "ops": {
                asset.key.to_user_string(): {
                    "config": {"poll_interval_seconds": 0.01, **config}
                }
            }
        },
        instance=instance,
        raise_on_error=instance is None,
    )


def seed(client, table=INPUT_TABLES[2]):
    client.execute(f"""INSERT INTO {table} (domain, website_url, priority, enabled, revision)
        VALUES ('a.example', 'https://a.example/', 20, true, 1),
               ('z.example', 'https://z.example/', 90, true, 1),
               ('off.example', 'https://off.example/', 100, false, 1)""")


@pytest.mark.parametrize(
    ("asset", "input_table", "kind"),
    list(zip(ASSETS, INPUT_TABLES, ("full", "jobs", "site_info"), strict=True)),
)
def test_results_wait_for_crawls_and_store_sections(
    database, crawler, asset, input_table, kind
):
    client, _, _ = database
    saved, calls, _ = crawler
    seed(client, input_table)
    assert run(
        database,
        asset,
        batch_size=1,
        max_batches=2,
        challenge_agent_model="z-ai/glm-5.3-flash",
        challenge_agent_max_runs=6,
        max_pages=1 if kind == "site_info" else 7,
    ).success
    assert [body["url"] for path, body in calls if path == "/v1/crawls"] == [
        "https://z.example/",
        "https://a.example/",
    ]
    assert len(saved) == 2
    for payload in saved.values():
        assert payload["challenge_agent_model"] == "z-ai/glm-5.3-flash"
        assert payload["challenge_agent_max_runs"] == 6
        assert payload["config"]["max_pages"] == (1 if kind == "site_info" else 7)
        if kind == "site_info":
            assert payload["crawl"] is False and payload["site_info"] is True
    rows = client.execute(
        f"SELECT domain, successful, site_info, page_observations, s3_path FROM {RESULTS_BY_TYPE[kind]} FINAL ORDER BY domain"
    )
    assert len(rows) == 2
    assert rows[0][0:2] == ("a.example", True)
    assert json.loads(rows[0][2])["description"] == "Makes sensors"
    assert "hi@example.org" in rows[0][3]
    assert rows[0][4].endswith("/attempts/0001/result.json.gz")
    assert run(database, asset, max_pages=1 if kind == "site_info" else 7).success
    assert len(saved) == 2  # Model for CAPTCHA does not change content freshness.


@pytest.mark.parametrize("batch_size", [1, 10])
def test_trailing_fresh_inputs_are_counted_once(database, crawler, batch_size):
    client, _, _ = database
    saved, _, _ = crawler
    seed(client)
    assert run(database, domains=["a.example"]).success
    # z is admitted first; a is a fresh input after the last admission.
    result = run(
        database,
        domains=["z.example", "a.example"],
        batch_size=batch_size,
        max_batches=2,
    )
    assert result.success
    [event] = result.get_asset_materialization_events()
    metadata = event.event_specific_data.materialization.metadata
    assert metadata["completed"].value == 1
    assert metadata["fresh_skipped"].value == 1
    assert len(saved) == 2


def test_partial_does_not_satisfy_freshness_and_later_failure_does_not_hide_success(
    database, crawler
):
    client, _, _ = database
    saved, _, behavior = crawler
    seed(client)
    behavior["partial"] = True
    assert run(database, domains=["a.example"]).success
    assert client.execute(
        "SELECT successful FROM corpscout.website_site_info_results FINAL"
    ) == [(False,)]
    behavior["partial"] = False
    assert run(database, domains=["a.example"]).success
    behavior["partial"] = True
    assert run(database, domains=["a.example"], force_refresh=True).success
    assert len(saved) == 3
    assert run(database, domains=["a.example"]).success
    assert len(saved) == 3
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results_latest_success"
    ) == [(1,)]


@pytest.mark.parametrize("waiting", ["pending", "s3_pending"])
def test_timeout_recovers_original_request_and_payload(database, crawler, waiting):
    client, _, _ = database
    saved, _, behavior = crawler
    seed(client)
    behavior[waiting] = True
    with pytest.raises(TimeoutError):
        run(
            database,
            domains=["a.example"],
            wait_timeout_seconds=0.01,
            challenge_agent_max_runs=6,
        )
    assert len(saved) == 1
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL"
    ) == [(0,)]
    behavior[waiting] = False
    # Even a different run/batch and model override resumes the saved request first.
    assert run(
        database, domains=["a.example"], challenge_agent_max_runs=12, force_refresh=True
    ).success
    assert len(saved) == 1
    assert next(iter(saved.values()))["challenge_agent_max_runs"] == 6
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL"
    ) == [(1,)]


def test_validation_precedes_receipt_and_crawl_submission(database, crawler):
    client, _, _ = database
    _, calls, behavior = crawler
    seed(client)
    behavior["reject"] = True
    with pytest.raises(ValueError, match="validation"):
        run(database)
    assert all(path.endswith("/validate") for path, _ in calls)
    assert client.execute(f"SELECT count() FROM {SUBMISSIONS}") == [(0,)]


def test_same_batch_replay_is_idempotent_and_semantic_change_is_due(database, crawler):
    client, _, _ = database
    saved, _, _ = crawler
    seed(client)
    batch = str(uuid4())
    assert run(
        database, batch_id=batch, domains=["a.example"], force_refresh=True
    ).success
    assert run(
        database, batch_id=batch, domains=["a.example"], force_refresh=True
    ).success
    assert len(saved) == 1
    assert run(database, domains=["a.example"], model="new-model").success
    assert len(saved) == 2


def test_cross_run_lock_blocks_second_processor(
    database,
    crawler,
    processing_postgres_url,  # noqa: F811
):
    client, _, _ = database
    _, calls, _ = crawler
    seed(client)
    with closing(psycopg2.connect(processing_postgres_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_lock(hashtextextended('website_crawl:site_info', 0))"
            )
        with pytest.raises(ValueError, match="already running"):
            run(database)
    assert not calls


@pytest.mark.parametrize(
    "field",
    [
        "challenge_agent_model",
        "challenge_agent_max_runs",
        "api",
        "model",
        "max_pages",
        "max_model_calls",
        "page_selection",
    ],
)
def test_execution_settings_are_required(field):
    from pydantic import ValidationError

    values = {
        "challenge_agent_model": "deepseek-flash",
        "challenge_agent_max_runs": 3,
        "api": "deepseek",
        "model": "deepseek-flash",
        "max_pages": 1,
        "max_model_calls": 20,
        "page_selection": "basic_info",
    }
    del values[field]
    with pytest.raises(ValidationError):
        CrawlResultsConfig(**values)


def test_refresh_policy_and_disabled_revisions(database, crawler):
    client, _, _ = database
    saved, _, _ = crawler
    seed(client)
    assert run(database, domains=["a.example"]).success
    client.execute("""INSERT INTO corpscout.website_site_info_results
        SELECT * REPLACE (now64(6) - INTERVAL 10 DAY AS finished_at, now64(6) AS ingested_at)
        FROM corpscout.website_site_info_results FINAL""")
    assert run(database, domains=["a.example"], refresh_interval_days=30).success
    assert len(saved) == 1
    assert run(database, domains=["a.example"], refresh_interval_days=7).success
    assert len(saved) == 2
    client.execute("""INSERT INTO corpscout.website_site_info_requests
        SELECT * EXCEPT bucket REPLACE (false AS enabled, 2 AS revision)
        FROM corpscout.website_site_info_requests_current WHERE domain='z.example'""")
    assert run(database).success
    assert len(saved) == 2


def test_custom_page_instructions_reach_crawler(database, crawler):
    client, _, _ = database
    saved, _, _ = crawler
    seed(client, INPUT_TABLES[0])
    assert run(
        database,
        website_full_crawl_results,
        domains=["a.example"],
        page_selection="instructions",
        instructions="Find all financial documents",
    ).success
    payload = next(iter(saved.values()))
    assert payload["instructions"] == "Find all financial documents"
    assert "crawl" not in payload


def test_sweep_resume_keeps_content_settings_fixed(database, crawler):
    client, _, _ = database
    seed(client)
    with dg.DagsterInstance.ephemeral() as instance:
        first = run(database, instance=instance)
        assert first.success
        resumed = run(
            database,
            instance=instance,
            execution_id=first.run_id,
            model="another-model",
        )
        assert not resumed.success
        assert "resume must keep model unchanged" in str(
            resumed.get_step_failure_events()[0].event_specific_data.error
        )
        assert run(database, instance=instance, execution_id=first.run_id).success


def test_resume_of_retired_task_execution_is_rejected(database, crawler):
    client, _, _ = database
    seed(client)
    with dg.DagsterInstance.ephemeral() as instance:
        first = run(database, instance=instance)
        assert first.success
        execution = json.loads(instance.get_run_by_id(first.run_id).tags[EXECUTION_TAG])
        instance.add_run_tags(
            first.run_id,
            {EXECUTION_TAG: json.dumps({**execution, "task_id": str(uuid4())})},
        )
        resumed = run(database, instance=instance, execution_id=first.run_id)
        assert not resumed.success
        assert "execution_id belongs to a retired crawl task" in str(
            resumed.get_step_failure_events()[0].event_specific_data.error
        )
