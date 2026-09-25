"""Draft import, immutable execution and cleanup against disposable PG/ClickHouse."""

from pathlib import Path
from uuid import uuid4

import dagster as dg
import pytest

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.input import INPUT_TABLES, TASK_DOMAINS
from dagster_v3.defs.website_crawl.queue_input import (
    CrawlQueueInputConfig,
    load_crawl_draft,
)
from dagster_v3.defs.website_crawl.results_assets import website_site_info_results
from tests.test_processing_store import processing_postgres_url, store  # noqa: F401
from tests.test_website_crawl_input_assets import server  # noqa: F401
from tests.test_website_crawl_results import crawler as crawler
from tests.test_webtech_input import objects  # noqa: F401

SETTINGS = {
    "challenge_agent_model": "deepseek-flash",
    "challenge_agent_max_runs": 3,
    "api": "deepseek",
    "model": "deepseek-flash",
    "max_pages": 1,
    "max_model_calls": 20,
    "page_selection": "basic_info",
    "poll_interval_seconds": 0.01,
}


@pytest.fixture
def db(server, store, objects):  # noqa: F811
    client, resource = server
    processing, dsn = store
    migrations = Path(__file__).parents[3] / "clickhouse/migrations"
    for name in (
        "000430_corpscout_website_crawl_type_results",
        "000431_corpscout_website_crawl_task_domains",
        "000445_corpscout_crawl_draft_queue",
    ):
        for statement in (migrations / f"{name}.up.sql").read_text().split(";"):
            if statement.strip():
                client.execute(statement)
    for table in (
        *INPUT_TABLES,
        TASK_DOMAINS,
        "corpscout.website_crawl_submissions",
        "corpscout.website_site_info_results",
    ):
        client.execute(f"TRUNCATE TABLE {table}")
    return client, resource, processing, ProcessingResource(postgres_url=dsn), objects


def add(db, submission_id=None, **kwargs):  # noqa: F811
    _, resource, processing, _, object_store = db
    return load_crawl_draft(
        CrawlQueueInputConfig(crawl_type="site_info", **kwargs),
        submission_id or str(uuid4()),
        processing,
        resource,
        object_store,
    )


def run(db, task_id, **overrides):  # noqa: F811
    _, resource, _, processing, object_store = db
    return dg.materialize(
        [
            website_site_info_results,
            dg.AssetSpec("website_crawl_input"),
        ],
        resources={
            "clickhouse": resource,
            "processing": processing,
            "crawler_queue_store": object_store,
        },
        run_config={
            "ops": {
                "website_site_info_results": {
                    "config": {**SETTINGS, "task_id": task_id, **overrides}
                }
            }
        },
    )


def test_append_dedup_source_manual_and_receipt_replay(db):
    client, _, processing, _, _ = db
    client.execute("DROP TABLE IF EXISTS corpscout.crawl_queue_source")
    client.execute(
        "CREATE TABLE corpscout.crawl_queue_source (domain String, country String) ENGINE=MergeTree ORDER BY domain"
    )
    client.execute(
        "INSERT INTO corpscout.crawl_queue_source VALUES ('one.example','SE'),('two.example','SE'),('outside.example','NO')"
    )
    submission = str(uuid4())
    first = add(
        db,
        submission,
        source_relation="corpscout.crawl_queue_source",
        filters={"country": ["SE"]},
    )
    second = add(db, targets=["two.example", "https://three.example/path"])
    assert first["task_id"] == second["task_id"]
    assert second["total"] == 3
    assert processing.task(first["task_id"])["status"] == "draft"
    client.execute(
        "INSERT INTO corpscout.crawl_queue_source VALUES ('four.example','SE')"
    )
    replay = add(
        db,
        submission,
        source_relation="corpscout.crawl_queue_source",
        filters={"country": ["SE"]},
    )
    assert replay["input_count"] == 2
    assert client.execute(
        f"SELECT domain FROM {TASK_DOMAINS} FINAL ORDER BY domain"
    ) == [("one.example",), ("three.example",), ("two.example",)]
    with pytest.raises(ValueError, match="different selection"):
        add(db, submission, targets=["other.example"])


@pytest.mark.parametrize("partial", [False, True])
def test_completion_saves_outcomes_clears_only_its_queue_and_replay_does_not_scan(
    db, crawler, partial
):  # noqa: F811
    client, _, processing, _, _ = db
    saved, calls, behavior = crawler
    behavior["partial"] = partial
    receipt = str(uuid4())
    task = add(db, receipt, targets=["one.example", "two.example"])["task_id"]
    result = run(db, task)
    metadata = result.asset_materializations_for_node("website_site_info_results")[
        0
    ].metadata
    assert metadata["completion_status"].value == (
        "completed_with_errors" if partial else "completed"
    )
    assert len(saved) == 2
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS} FINAL") == [(0,)]
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL"
    ) == [(2,)]
    record = processing.task(task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is not None
    next_task = add(db, targets=["three.example"])["task_id"]
    assert task != next_task
    before = list(calls)
    assert run(db, task).success
    assert calls == before
    assert add(db, receipt, targets=["one.example", "two.example"])["task_id"] == task
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS} FINAL") == [
        ("three.example",)
    ]


def test_timeout_keeps_inputs_and_resumes_saved_profile_with_new_draft(db, crawler):  # noqa: F811
    client, _, processing, _, _ = db
    saved, _, behavior = crawler
    task = add(db, targets=["one.example", "two.example"])["task_id"]
    behavior["pending"] = True
    with pytest.raises(TimeoutError):
        run(db, task, wait_timeout_seconds=0.05)
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS} FINAL") == [(2,)]
    submitted = dict(saved)
    later = add(db, targets=["next.example"])["task_id"]
    assert later != task
    with pytest.raises(ValueError, match="settings are frozen"):
        run(db, task, model="other")
    behavior["pending"] = False
    assert run(db, task).success
    assert saved == submitted
    assert processing.task(later)["status"] == "draft"
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS} FINAL") == [
        ("next.example",)
    ]


def test_freshness_is_at_execution_and_force_can_override(db, crawler):  # noqa: F811
    _, _, processing, _, _ = db
    saved, _, _ = crawler
    first = add(db, targets=["one.example"])["task_id"]
    assert run(db, first).success
    second = add(db, targets=["one.example"])["task_id"]
    assert processing.task(second)["total"] == 1
    assert run(db, second).success
    assert processing.task(second)["skipped_count"] == 1
    assert len(saved) == 1
    third = add(db, targets=["one.example"])["task_id"]
    assert run(db, third, force_refresh=True).success
    assert len(saved) == 2


def test_lost_import_ack_replays_snapshot_and_blocks_start_until_repaired(
    db, crawler, monkeypatch
):
    from clickhouse_driver import Client

    client, _, processing, _, _ = db
    client.execute("DROP TABLE IF EXISTS corpscout.crawl_retry_source")
    client.execute(
        "CREATE TABLE corpscout.crawl_retry_source (domain String) ENGINE=MergeTree ORDER BY domain"
    )
    client.execute("INSERT INTO corpscout.crawl_retry_source VALUES ('one.example')")
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith(f"INSERT INTO {TASK_DOMAINS}") and not interrupted:
            interrupted = True
            raise ConnectionError("lost insert acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    submission = str(uuid4())
    config = {"source_relation": "corpscout.crawl_retry_source", "select_all": True}
    with pytest.raises(ConnectionError, match="acknowledgement"):
        add(db, submission, **config)
    [(task,)] = client.execute(f"SELECT task_id FROM {TASK_DOMAINS} FINAL")
    with pytest.raises(ValueError, match="outstanding imports"):
        run(db, task)
    client.execute("INSERT INTO corpscout.crawl_retry_source VALUES ('later.example')")
    assert add(db, submission, **config)["total"] == 1
    assert processing.task(task)["status"] == "draft"
    assert run(db, task).success


def test_lost_cleanup_ack_does_not_repeat_crawls(db, crawler, monkeypatch):
    from clickhouse_driver import Client

    _, _, processing, _, _ = db
    _, calls, _ = crawler
    task = add(db, targets=["one.example"])["task_id"]
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith(f"DELETE FROM {TASK_DOMAINS}") and not interrupted:
            interrupted = True
            raise ConnectionError("lost cleanup acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    with pytest.raises(ConnectionError, match="acknowledgement"):
        run(db, task)
    assert processing.task(task)["status"] == "completed"
    assert processing.task(task)["inputs_purged_at"] is None
    before = list(calls)
    assert run(db, task).success
    assert calls == before
    assert processing.task(task)["inputs_purged_at"] is not None


def test_task_and_explicit_domains_are_exclusive():
    from pydantic import ValidationError
    from dagster_v3.defs.website_crawl.results import CrawlResultsConfig

    with pytest.raises(ValidationError, match="task_id"):
        CrawlResultsConfig(**SETTINGS, task_id=str(uuid4()), domains=["a.example"])
