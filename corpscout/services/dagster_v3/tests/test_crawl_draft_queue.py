"""Draft import, immutable execution and cleanup against disposable PG/ClickHouse."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import dagster as dg
import pytest

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.dispatch import crawl_payload
from dagster_v3.defs.website_crawl.input import (
    INPUT_TABLES,
    TASK_DOMAINS,
    task_processor,
)
from dagster_v3.defs.website_crawl.queue_execution import (
    REQUEST_ID_SQL,
    count_unresolved,
    dispatchable_entries,
    remaining_crawl_entries,
    start_crawl_execution,
)
from dagster_v3.defs.website_crawl.queue_input import (
    CrawlQueueInputConfig,
    load_crawl_draft,
)
from dagster_v3.defs.website_crawl.results import CrawlResultsConfig
from dagster_v3.defs.website_crawl.results_assets import website_site_info_results
from tests.test_processing_store import processing_postgres_url, store  # noqa: F401
from tests.test_website_crawl_input_assets import server  # noqa: F401
from tests.test_website_crawl_results import crawler as crawler

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
def db(server, store):  # noqa: F811
    client, resource = server
    processing, dsn = store
    migrations = Path(__file__).parents[3] / "clickhouse/migrations"
    for statement in (
        (migrations / "000430_corpscout_website_crawl_type_results.up.sql")
        .read_text()
        .split(";")
    ):
        if statement.strip():
            client.execute(statement)
    for table in (
        *INPUT_TABLES,
        TASK_DOMAINS,
        "corpscout.website_crawl_submissions",
        "corpscout.website_site_info_results",
    ):
        client.execute(f"TRUNCATE TABLE {table}")
    return client, resource, processing, ProcessingResource(postgres_url=dsn)


def add(db, submission_id=None, **kwargs):  # noqa: F811
    _, resource, processing, _ = db
    return load_crawl_draft(
        CrawlQueueInputConfig(crawl_type="site_info", **kwargs),
        submission_id or str(uuid4()),
        processing,
        resource,
    )


def run(db, task_id, **overrides):  # noqa: F811
    _, resource, _, processing = db
    return dg.materialize(
        [
            website_site_info_results,
            dg.AssetSpec("website_crawl_input"),
        ],
        resources={"clickhouse": resource, "processing": processing},
        run_config={
            "ops": {
                "website_site_info_results": {
                    "config": {**SETTINGS, "task_id": task_id, **overrides}
                }
            }
        },
    )


def start(db, task_id, **overrides):
    """Freeze the draft the way the results asset does, without crawling."""
    _, resource, processing, _ = db
    config = CrawlResultsConfig(**SETTINGS, **overrides)
    with processing.selection_lock(task_id), resource.get_connection() as client:
        task = start_crawl_execution(
            processing, client, task_id, "site_info", config, str(uuid4())
        )
    return task, config


def publish(client, *, domain, request_id, run_id, work_key, successful, finished_at):
    client.execute(
        "INSERT INTO corpscout.website_site_info_results (domain,website_url,request_id,attempt,input_revision,work_key,run_id,state,crawl_status,successful,finished_at,error,s3_path,s3_state) VALUES",
        [
            (
                domain,
                f"https://{domain}/",
                request_id,
                1,
                1,
                work_key,
                run_id,
                "completed",
                "finished",
                successful,
                finished_at,
                "",
                "",
                "uploaded",
            )
        ],
    )


def test_append_dedup_source_manual_and_receipt_replay(db):
    client, _, processing, _ = db
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
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS} ORDER BY domain") == [
        ("one.example",),
        ("three.example",),
        ("two.example",),
    ]
    with pytest.raises(ValueError, match="different selection"):
        add(db, submission, targets=["other.example"])


@pytest.mark.parametrize("partial", [False, True])
def test_completion_counts_results_drops_its_partition_and_replay_does_not_crawl(
    db, crawler, partial
):
    client, _, processing, _ = db
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
    assert metadata["stored_results"].value == 2
    assert len(saved) == 2
    assert [path for path, _ in calls] == ["/v1/crawls", "/v1/crawls"]
    execution = processing.task(task)["config"]["execution"]
    # Results carry this execution's request ids and run id; nothing else is stored.
    assert client.execute(
        "SELECT domain, request_id, run_id FROM corpscout.website_site_info_results FINAL ORDER BY domain"
    ) == [
        (domain, saved_id, execution["execution_id"])
        for domain, saved_id in sorted(
            (payload["url"].removeprefix("https://").rstrip("/"), request_id)
            for request_id, payload in saved.items()
        )
    ]
    assert client.execute(
        "SELECT count() FROM corpscout.website_crawl_submissions"
    ) == [(0,)]
    assert client.execute(
        f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s", {"task": task}
    ) == [(0,)]
    record = processing.task(task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is not None
    assert (
        record["succeeded_count"],
        record["terminal_failed_count"],
        record["skipped_count"],
    ) == ((0, 2, 0) if partial else (2, 0, 0))
    next_task = add(db, targets=["three.example"])["task_id"]
    assert task != next_task
    before = list(calls)
    assert run(db, task).success
    assert calls == before
    assert add(db, receipt, targets=["one.example", "two.example"])["task_id"] == task
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS}") == [("three.example",)]


def test_timeout_keeps_inputs_and_resume_polls_the_same_requests(db, crawler):
    client, _, processing, _ = db
    saved, calls, behavior = crawler
    task = add(db, targets=["one.example", "two.example"])["task_id"]
    behavior["pending"] = True
    with pytest.raises(TimeoutError):
        run(db, task, wait_timeout_seconds=0.05)
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS}") == [(2,)]
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results"
    ) == [(0,)]
    submitted, created = dict(saved), list(behavior["created"])
    later = add(db, targets=["next.example"])["task_id"]
    assert later != task
    with pytest.raises(ValueError, match="settings are frozen"):
        run(db, task, model="other")
    behavior["pending"] = False
    assert run(db, task, max_in_flight=1).success  # transport settings may change
    # The resume re-sent the same requests and reattached: no duplicate crawl work.
    assert saved == submitted and behavior["created"] == created
    assert processing.task(task)["status"] == "completed"
    assert processing.task(later)["status"] == "draft"
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS}") == [("next.example",)]


def test_freshness_is_at_execution_and_force_can_override(db, crawler):  # noqa: F811
    _, _, processing, _ = db
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


def test_lost_import_ack_blocks_start_until_retried_from_the_current_source(
    db, crawler, monkeypatch
):
    from clickhouse_driver import Client

    client, _, processing, _ = db
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
    [(task,)] = client.execute(f"SELECT DISTINCT task_id FROM {TASK_DOMAINS}")
    with pytest.raises(ValueError, match="outstanding imports"):
        run(db, task)
    client.execute("INSERT INTO corpscout.crawl_retry_source VALUES ('later.example')")
    # No manifest freezes the first attempt: the retry reselects the current source.
    result = add(db, submission, **config)
    assert (result["total"], result["input_count"]) == (2, 2)
    assert client.execute(
        f"SELECT domain, submission_id FROM {TASK_DOMAINS} ORDER BY domain"
    ) == [("later.example", submission), ("one.example", submission)]
    assert draft_queue.submission(processing, submission)["manifest_uri"] is None
    assert processing.task(task)["status"] == "draft"
    assert run(db, task).success


def test_lost_cleanup_ack_does_not_repeat_crawls(db, crawler, monkeypatch):
    from clickhouse_driver import Client

    _, _, processing, _ = db
    _, calls, _ = crawler
    task = add(db, targets=["one.example"])["task_id"]
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if (
            query.startswith(f"ALTER TABLE {TASK_DOMAINS} DROP PARTITION")
            and not interrupted
        ):
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


def test_window_bounds_in_flight_requests_and_batches_result_writes(
    db, crawler, monkeypatch
):
    from clickhouse_driver import Client

    client, _, processing, _ = db
    saved, calls, _ = crawler
    task = add(db, targets=[f"d{n}.example" for n in range(1, 6)])["task_id"]
    execute = Client.execute
    inserts = []

    def counting(self, query, *args, **kwargs):
        if query.startswith("INSERT INTO corpscout.website_site_info_results"):
            inserts.append(len(args[0]))
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", counting)
    assert run(db, task, max_in_flight=2).success
    assert len(saved) == 5 and [path for path, _ in calls].count("/v1/crawls") == 5
    # Two outstanding at a time; a window's outcomes are stored in one acknowledged insert.
    assert inserts == [2, 2, 1]
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL"
    ) == [(5,)]
    assert processing.task(task)["succeeded_count"] == 5


def test_preset_changed_after_its_request_refuses_the_resume(db, crawler):
    client, _, processing, _ = db
    _, _, behavior = crawler
    task = add(db, targets=["one.example"])["task_id"]
    behavior["pending"] = True
    with pytest.raises(TimeoutError):
        run(db, task, wait_timeout_seconds=0.05)
    client.execute(
        "INSERT INTO corpscout.website_site_info_requests SELECT * EXCEPT bucket REPLACE (NOT save_artifacts AS save_artifacts, 2 AS revision) FROM corpscout.website_site_info_requests_current WHERE domain='one.example'"
    )
    behavior["pending"] = False
    with pytest.raises(ValueError, match="preset for one.example changed after"):
        run(db, task)
    # The old crawl is never stored under the new preset; the entry stays queued.
    assert len(behavior["created"]) == 1
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results"
    ) == [(0,)]
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS}") == [(1,)]
    assert processing.task(task)["status"] == "selected"


def test_request_lost_by_the_crawler_is_sent_again(db, crawler):
    client, _, processing, _ = db
    saved, calls, behavior = crawler
    behavior["forget"] = True
    task = add(db, targets=["one.example"])["task_id"]
    assert run(db, task).success
    [request_id] = saved
    # Polling found no job (a crawler restart) and re-sent the same identity.
    assert behavior["created"] == [request_id, request_id]
    assert [path for path, _ in calls] == ["/v1/crawls", "/v1/crawls"]
    assert client.execute(
        "SELECT request_id FROM corpscout.website_site_info_results FINAL"
    ) == [(request_id,)]
    assert processing.task(task)["succeeded_count"] == 1


def test_failed_job_without_a_crawl_object_is_a_failed_outcome(db, crawler):
    client, _, processing, _ = db
    _, _, behavior = crawler
    behavior["state"] = "failed"
    behavior["result"] = {"error": "browser crashed"}
    task = add(db, targets=["one.example"])["task_id"]
    result = run(db, task)
    metadata = result.asset_materializations_for_node("website_site_info_results")[
        0
    ].metadata
    assert metadata["completion_status"].value == "completed_with_errors"
    assert metadata["failed_pages"].value == 1
    assert client.execute(
        "SELECT state, successful FROM corpscout.website_site_info_results FINAL"
    ) == [("failed", False)]
    record = processing.task(task)
    assert (record["succeeded_count"], record["terminal_failed_count"]) == (0, 1)


def test_result_not_ready_yet_is_polled_again(db, crawler):
    _, _, processing, _ = db
    _, _, behavior = crawler
    behavior["result_not_ready"] = 2
    task = add(db, targets=["one.example"])["task_id"]
    assert run(db, task).success
    assert behavior["result_not_ready"] == 0
    assert processing.task(task)["succeeded_count"] == 1


def test_failed_flush_while_polling_keeps_outcomes_for_the_next_flush(
    db, crawler, monkeypatch
):
    from clickhouse_driver import Client

    client, _, processing, _ = db
    task = add(db, targets=["one.example", "two.example"])["task_id"]
    execute = Client.execute
    attempts = []

    def flaky(self, query, *args, **kwargs):
        if query.startswith("INSERT INTO corpscout.website_site_info_results"):
            attempts.append(len(args[0]))
            if len(attempts) == 1:
                raise ConnectionError("insert not acknowledged")
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", flaky)
    assert run(db, task).success
    # The failed insert kept both rows; the next flush stored the same batch.
    assert attempts == [2, 2]
    assert client.execute(
        "SELECT count() FROM corpscout.website_site_info_results FINAL"
    ) == [(2,)]
    assert processing.task(task)["succeeded_count"] == 2


def test_task_and_explicit_domains_are_exclusive():
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="task_id"):
        CrawlResultsConfig(**SETTINGS, task_id=str(uuid4()), domains=["a.example"])


def test_task_id_must_name_a_draft(db, crawler):  # noqa: F811
    _, _, processing, _ = db
    _, calls, _ = crawler
    with pytest.raises(ValueError, match="unknown crawl task"):
        run(db, str(uuid4()))
    legacy = str(uuid4())
    processing.prepare_selection(
        legacy, processor=task_processor("site_info"), fingerprint="legacy"
    )
    assert processing.task(legacy)["queue_scope"] is None
    with pytest.raises(ValueError, match="unknown crawl task"):
        run(db, legacy)
    assert calls == []


def test_entry_table_follows_the_queue_contract(db):
    from clickhouse_driver.errors import ServerException

    client, *_ = db
    assert client.execute(
        "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='website_crawl_task_domains'"
    ) == [("MergeTree", "task_id", "task_id, domain")]
    # Every new row names its submission; the retry delete relies on it.
    with pytest.raises(ServerException, match="valid_task"):
        client.execute(
            f"INSERT INTO {TASK_DOMAINS} (task_id,crawl_type,domain,website_url,source_name) VALUES",
            [("task", "full", "a.example", "https://a.example/", "manual")],
        )


def test_retry_replaces_only_its_own_rows(db, monkeypatch):
    from clickhouse_driver import Client

    client, _, _, _ = db
    other = add(db, targets=["kept.example"])
    submission = str(uuid4())
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
    with pytest.raises(ConnectionError):
        add(db, submission, targets=["mine.example"])
    result = add(db, submission, targets=["mine.example"])
    assert result["task_id"] == other["task_id"] and result["total"] == 2
    # The retry deleted and re-inserted its own row; the sibling's row is untouched.
    assert client.execute(
        f"SELECT domain, submission_id FROM {TASK_DOMAINS} ORDER BY domain"
    ) == [("kept.example", other["submission_id"]), ("mine.example", submission)]


def test_request_id_matches_between_sql_and_python(db):
    client, *_ = db
    execution = str(uuid4())
    [(from_sql,)] = client.execute(
        f"SELECT {REQUEST_ID_SQL} FROM (SELECT 'one.example' AS domain)",
        {"exec": execution, "type": "site_info"},
    )
    row = {
        "preset_version": 1,
        "proxy_route": "direct",
        "config_json": "{}",
        "domain": "one.example",
        "website_url": "https://one.example/",
        "save_artifacts": True,
        "headless": True,
        "page_mode": "discover",
        "pages": [],
        "instructions": "",
    }
    assert crawl_payload(row, "site_info", execution)["request_id"] == from_sql
    assert from_sql.startswith("dagster-crawl-") and len(from_sql) == 14 + 64


def test_remaining_excludes_own_results_fresh_successes_and_disabled_presets(db):
    client, resource, _, _ = db
    task_id = add(db, targets=["one.example", "two.example", "three.example"])[
        "task_id"
    ]
    task, config = start(db, task_id)
    execution = task["config"]["execution"]
    started_at = datetime.fromisoformat(execution["started_at"])
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        items = {
            item["domain"]: item
            for item in dispatchable_entries(
                connection, rows, task=task, crawl_type="site_info", config=config
            )
        }
    assert sorted(items) == ["one.example", "three.example", "two.example"]
    assert all(item["run_id"] == execution["execution_id"] for item in items.values())
    assert all(
        item["request_id"].startswith("dagster-crawl-") for item in items.values()
    )
    # one: this execution's own result, even a failure, leaves the remaining set.
    publish(
        client,
        domain="one.example",
        request_id=items["one.example"]["request_id"],
        run_id=execution["execution_id"],
        work_key=items["one.example"]["work_key"],
        successful=False,
        finished_at=datetime.now(UTC),
    )
    # two: an older failure then a success inside the frozen window -> fresh.
    publish(
        client,
        domain="two.example",
        request_id="other-1",
        run_id="other",
        work_key=items["two.example"]["work_key"],
        successful=False,
        finished_at=started_at - timedelta(hours=2),
    )
    publish(
        client,
        domain="two.example",
        request_id="other-2",
        run_id="other",
        work_key=items["two.example"]["work_key"],
        successful=True,
        finished_at=started_at - timedelta(hours=1),
    )
    # three: a success after the execution started never counts as fresh.
    publish(
        client,
        domain="three.example",
        request_id="other-3",
        run_id="other",
        work_key=items["three.example"]["work_key"],
        successful=True,
        finished_at=started_at + timedelta(hours=1),
    )
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        assert [row["domain"] for row in rows] == ["three.example", "two.example"]
        dispatchable = dispatchable_entries(
            connection, rows, task=task, crawl_type="site_info", config=config
        )
        assert [item["domain"] for item in dispatchable] == ["three.example"]
        assert count_unresolved(connection, task, "site_info", config) == 1
        # A later failure inside the window never hides the earlier success.
        publish(
            client,
            domain="two.example",
            request_id="other-4",
            run_id="other",
            work_key=items["two.example"]["work_key"],
            successful=False,
            finished_at=started_at - timedelta(minutes=30),
        )
        rows = remaining_crawl_entries(connection, task, "site_info")
        assert [
            item["domain"]
            for item in dispatchable_entries(
                connection, rows, task=task, crawl_type="site_info", config=config
            )
        ] == ["three.example"]
        # A disabled preset is a skip, not work; pages are read with a cursor.
        client.execute(
            "INSERT INTO corpscout.website_site_info_requests SELECT * EXCEPT bucket REPLACE (false AS enabled, 2 AS revision) FROM corpscout.website_site_info_requests_current WHERE domain='three.example'"
        )
        assert [
            row["domain"]
            for row in remaining_crawl_entries(
                connection, task, "site_info", after="three.example", limit=1
            )
        ] == ["two.example"]
        publish(
            client,
            domain="two.example",
            request_id=items["two.example"]["request_id"],
            run_id=execution["execution_id"],
            work_key=items["two.example"]["work_key"],
            successful=True,
            finished_at=datetime.now(UTC),
        )
        assert [
            row["domain"]
            for row in remaining_crawl_entries(connection, task, "site_info")
        ] == ["three.example"]
        assert count_unresolved(connection, task, "site_info", config) == 0


def test_force_refresh_disables_only_the_freshness_skip(db):
    client, resource, _, _ = db
    task_id = add(db, targets=["one.example"])["task_id"]
    task, config = start(db, task_id, force_refresh=True)
    started_at = datetime.fromisoformat(task["config"]["execution"]["started_at"])
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        [item] = dispatchable_entries(
            connection, rows, task=task, crawl_type="site_info", config=config
        )
    publish(
        client,
        domain="one.example",
        request_id="other-1",
        run_id="other",
        work_key=item["work_key"],
        successful=True,
        finished_at=started_at - timedelta(hours=1),
    )
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        assert (
            len(
                dispatchable_entries(
                    connection, rows, task=task, crawl_type="site_info", config=config
                )
            )
            == 1
        )
        unforced = CrawlResultsConfig(**SETTINGS)
        assert (
            dispatchable_entries(
                connection, rows, task=task, crawl_type="site_info", config=unforced
            )
            == []
        )
