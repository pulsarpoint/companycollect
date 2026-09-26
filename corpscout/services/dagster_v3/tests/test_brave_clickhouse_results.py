"""Direct outcome persistence and skip/rescan policy at real HTTP and database boundaries."""

from datetime import UTC, datetime, timedelta
from itertools import count
from uuid import uuid4

import dagster as dg
import pytest

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.company_domains import assets
from dagster_v3.defs.company_domains.browser import BraveBrowserResource
from dagster_v3.defs.company_domains.results import (
    RESULT_COLUMNS,
    RESULT_TABLE,
    insert_results,
    search_is_due,
)
from dagster_v3.defs.company_domains.migrate_results import migrate_results
from dagster_v3.defs.company_domains.publication import publish_results
from tests.test_brave_publication import (
    archive_s3 as archive_s3,
    clickhouse as clickhouse,
)
from tests.test_company_domains_brave import brave_api as brave_api
from tests.test_processing_store import prepare_task, claim, complete

pytest_plugins = ["tests.test_processing_store"]


@pytest.mark.parametrize(
    "force,rescan_old,age,expected",
    [
        (False, False, None, True),
        (False, False, 500, False),
        (False, True, 29, False),
        (False, True, 30, False),
        (False, True, 31, True),
        (True, False, 0, True),
        (True, True, 0, True),
    ],
)
def test_search_age_policy(force, rescan_old, age, expected):
    start = datetime(2026, 9, 20, tzinfo=UTC)
    assert (
        search_is_due(
            None if age is None else start - timedelta(days=age),
            force=force,
            rescan_old=rescan_old,
            started_at=start,
        )
        is expected
    )


def record(
    company, *, status="success", age=0, country="SE", query_type="official_website"
):
    return {
        "country_code": country,
        "company_id": company,
        "company_name": f"Company {company}",
        "query_type": query_type,
        "query": f"Find Company {company}",
        "result_id": uuid4(),
        "status": status,
        "answer_text": f"https://company{company}.test/" if status == "success" else "",
        "completed_at": datetime.now(UTC) - timedelta(days=age),
        "error_type": "TimeoutError" if status == "error" else "",
        "error_stage": "",
        "route": "direct",
        "source_url": "https://search.brave.com/ask",
        "elapsed_ms": 10,
        "answer_timeout_ms": 60000,
        "challenge_runs_json": "[]",
        "task_id": uuid4(),
        "execution_id": uuid4(),
        "source_run_id": str(uuid4()),
        "input_id": company,
        "attempt": 1,
        "processor_version": "brave-v2",
        "search_id": "",
        "search_revision": 0,
        "search_name": "",
    }


def inputs(client, count):
    client.execute("""CREATE TABLE corpscout.brave_test_input (
        input_id String, company_id String, company_name String, country_code String
    ) ENGINE=MergeTree ORDER BY input_id""")
    client.execute(
        "INSERT INTO corpscout.brave_test_input VALUES",
        [(str(i), str(i), f"Company {i}", "SE") for i in range(count)],
    )


def materialize(resource, dsn, fixture, instance, *, tags=None, **config):
    return dg.materialize(
        [assets.company_brave_search_results],
        instance=instance,
        tags=tags,
        resources={
            "clickhouse": resource,
            "processing_clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
            "company_brave_browser": BraveBrowserResource(**fixture.config),
        },
        run_config={"ops": {"company_brave_search_results": {"config": config}}},
        raise_on_error=False,
    )


def test_completed_results_skip_errors_rescan_age_and_force_without_postgres_items(
    store,
    clickhouse,
    brave_api,
):
    queue, dsn = store
    client, resource = clickhouse
    inputs(client, 5)
    fixture = brave_api()
    fixture.responder = lambda _: {}
    insert_results(
        client,
        [
            record("0"),
            record("1", status="error"),
            record("2", age=40),
            record("3", status="error", age=40),
            # A different country or search type does not suppress company 4's website query.
            record("4", country="NO"),
            record("4", query_type="owner"),
        ],
    )
    task = str(uuid4())
    with dg.DagsterInstance.ephemeral() as instance:
        normal = materialize(
            resource,
            dsn,
            fixture,
            instance,
            task_id=task,
            input_relation="corpscout.brave_test_input",
        )
        assert normal.success
        assert len(fixture.queries) == 1
        assert "Company 4" in fixture.queries[0]
        again = materialize(resource, dsn, fixture, instance, task_id=task)
        assert again.success and len(fixture.queries) == 1
        aged = materialize(
            resource, dsn, fixture, instance, task_id=task, rescan_old=True
        )
        assert aged.success and len(fixture.queries) == 3
        forced = materialize(resource, dsn, fixture, instance, task_id=task, force=True)
        assert forced.success and len(fixture.queries) == 8
        resumed = materialize(
            resource, dsn, fixture, instance, execution_id=forced.run_id
        )
        assert resumed.success and len(fixture.queries) == 8
        changed = materialize(
            resource, dsn, fixture, instance, execution_id=forced.run_id, force=False
        )
        assert not changed.success and len(fixture.queries) == 8
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT (SELECT count(*) FROM processing.items) AS items, (SELECT count(*) FROM processing.results) AS results"
        )
        assert dict(cursor.fetchone()) == {"items": 0, "results": 0}
    assert client.execute(f"SELECT count() FROM {RESULT_TABLE} FINAL") == [(14,)]
    assert client.execute(
        "SELECT count() FROM corpscout.se_company_brave_search_results_latest_success FINAL WHERE query_type='official_website'"
    ) == [(5,)]


@pytest.mark.parametrize("cadence", ["rows", "time"])
def test_progress_counts_saved_results_skips_and_resumed_execution(
    store, clickhouse, brave_api, monkeypatch, cadence
):
    _, dsn = store
    client, resource = clickhouse
    inputs(client, 6)
    insert_results(client, [record("0"), record("1", status="error")])
    fixture = brave_api()
    fixture.responder = lambda payload: (
        {"status": "error", "answer": "", "error_type": "TimeoutError"}
        if "Company 3" in payload["query"]
        else {}
    )
    # Advance only the reporter's clock; browser and database timing stay real.
    ticks = count(step=31 if cadence == "time" else 0)
    monkeypatch.setattr(assets, "monotonic", lambda: next(ticks))
    settings = {"progress_log_every": 2 if cadence == "rows" else 100_000}

    with dg.DagsterInstance.ephemeral() as instance:
        first = materialize(
            resource, dsn, fixture, instance,
            input_relation="corpscout.brave_test_input", **settings,
        )
        assert not first.success  # One search failed, but its outcome is durable.
        resumed = materialize(
            resource, dsn, fixture, instance, execution_id=first.run_id, **settings,
        )
        assert not resumed.success
        assert len(fixture.queries) == 4  # No repeated requests on resume.

        for result, initial_processed in [(first, 0), (resumed, 4)]:
            progress = [
                dict(token.split("=", 1) for token in event.message.split()[2:])
                for event in instance.all_logs(result.run_id)
                if event.message.startswith("Brave progress ")
            ]
            assert progress[0]["phase"] == "start"
            assert int(progress[0]["processed"]) == initial_processed
            assert int(progress[0]["remaining"]) == 6 - initial_processed
            assert progress[-1]["phase"] == "finished"
            assert any(row["phase"] == "running" for row in progress)
            for row in progress:
                assert row["execution"] == first.run_id
                assert int(row["total"]) == 6
                assert int(row["processed"]) == int(row["succeeded"]) + int(row["failed"])
                assert int(row["remaining"]) == 6 - int(row["processed"]) - int(row["skipped"])
                assert int(row["new_results"]) == int(row["processed"]) - initial_processed
            final = progress[-1]
            assert (final["processed"], final["succeeded"], final["failed"], final["skipped"], final["remaining"]) == ("4", "3", "1", "2", "0")
            assert final["progress"] == "100.00%"


def test_errors_are_saved_and_a_failed_rescan_preserves_previous_success(
    store, clickhouse, brave_api
):
    _, dsn = store
    client, resource = clickhouse
    inputs(client, 1)
    previous = record("0", age=40)
    insert_results(client, [previous])
    fixture = brave_api()
    fixture.responder = lambda _: {
        "status": "error",
        "answer": "",
        "error_type": "TimeoutError",
        "error_stage": "answer_generation",
        "challenge_runs": [{"state": "blocked"}],
    }
    task = str(uuid4())
    with dg.DagsterInstance.ephemeral() as instance:
        failed = materialize(
            resource,
            dsn,
            fixture,
            instance,
            task_id=task,
            input_relation="corpscout.brave_test_input",
            rescan_old=True,
        )
        assert not failed.success
        assert len(fixture.queries) == 1
        assert materialize(resource, dsn, fixture, instance, task_id=task).success
        assert len(fixture.queries) == 1
        fixture.responder = lambda _: {}
        assert materialize(
            resource, dsn, fixture, instance, task_id=task, force=True
        ).success
        assert len(fixture.queries) == 2
    assert client.execute(
        f"SELECT status, error_type, error_stage, challenge_runs_json FROM {RESULT_TABLE} FINAL WHERE status='error'"
    ) == [("error", "TimeoutError", "answer_generation", '[{"state": "blocked"}]')]
    assert client.execute(f"SELECT count() FROM {RESULT_TABLE} FINAL") == [(3,)]


def test_lost_acknowledgement_resumes_forced_execution_without_repeating_saved_work(
    store,
    clickhouse,
    brave_api,
    monkeypatch,
):
    _, dsn = store
    client, resource = clickhouse
    inputs(client, 12)
    fixture = brave_api()
    fixture.responder = lambda _: {}
    task = str(uuid4())
    actual_insert = assets.insert_results

    def lost_ack(writer, records):
        actual_insert(writer, records)
        raise ConnectionError("simulated lost insert acknowledgement")

    with dg.DagsterInstance.ephemeral() as instance:
        monkeypatch.setattr(assets, "insert_results", lost_ack)
        failed = materialize(
            resource,
            dsn,
            fixture,
            instance,
            task_id=task,
            input_relation="corpscout.brave_test_input",
            force=True,
        )
        assert not failed.success
        [(saved,)] = client.execute(f"SELECT count() FROM {RESULT_TABLE} FINAL")
        assert 1 <= saved <= 4
        monkeypatch.setattr(assets, "insert_results", actual_insert)
        assert materialize(
            resource, dsn, fixture, instance, execution_id=failed.run_id
        ).success
        assert len(fixture.queries) == 12
        assert len({payload["request_id"] for payload in fixture.payloads}) == 12
        assert client.execute(f"SELECT count() FROM {RESULT_TABLE} FINAL") == [(12,)]
        # Projection repair works independently of the input source and browser.
        client.execute("TRUNCATE TABLE corpscout.se_company_brave_search_results_latest_success")
        client.execute("DROP TABLE corpscout.brave_test_input")
        assert materialize(
            resource, dsn, fixture, instance, execution_id=failed.run_id, mode="publish"
        ).success
        assert client.execute(
            "SELECT count() FROM corpscout.se_company_brave_search_results_latest_success FINAL"
        ) == [(12,)]
        assert len(fixture.queries) == 12


def test_schema_retries_keep_attempt_history_and_latest_failed_status(clickhouse):
    client, _ = clickhouse
    older = record("42", age=40)
    newer = record("42", status="error")
    insert_results(client, [older, newer])
    insert_results(client, [older, newer])
    assert client.execute(f"SELECT count() FROM {RESULT_TABLE} FINAL") == [(2,)]
    assert client.execute(
        "SELECT status FROM corpscout.company_brave_search_results_latest WHERE company_id='42'"
    ) == [("error",)]
    assert client.execute(
        "SELECT answer_text, error_type FROM corpscout.company_brave_search_results_latest "
        "WHERE country_code='SE' AND company_id='42'"
    ) == [("", "TimeoutError")]
    assert client.execute(
        "SELECT count() FROM corpscout.company_brave_search_results_latest "
        "WHERE country_code='SE' AND company_id='42' AND status='success'"
    ) == [(0,)]
    assert client.execute(
        "SELECT answer_text FROM corpscout.se_company_brave_search_results_latest_success FINAL WHERE company_id='42'"
    ) == [(older["answer_text"],)]
    columns = client.execute(f"DESCRIBE TABLE {RESULT_TABLE}")
    assert tuple(row[0] for row in columns) == RESULT_COLUMNS


def test_cutover_imports_archived_and_unpublished_results_without_losing_diagnostics(
    store, clickhouse
):
    queue, _ = store
    client, _ = clickhouse
    task = prepare_task(queue, names=("Archived", "Unpublished"))
    with queue.transaction() as cursor:
        cursor.execute(
            'UPDATE processing.tasks SET config=config || \'{"query_type":"official_website"}\'::jsonb WHERE task_id=%s',
            (task,),
        )
    archived = complete(
        queue, claim(queue, task), answer="Archived åäö https://example.se"
    )
    with queue.transaction() as cursor:
        cursor.execute(
            'UPDATE processing.results SET payload=payload || \'{"elapsed_ms":123,"answer_timeout_ms":60000,"challenge_runs":[{"state":"solved"}]}\'::jsonb WHERE result_id=%s',
            (archived,),
        )
    assert publish_results(queue, client, task, batch_size=100) == 1
    unpublished = complete(queue, claim(queue, task), status="error", answer="")
    assert migrate_results(queue, client, batch_size=1) == {
        "postgres_results_verified": 2,
        "clickhouse_results": 2,
    }
    assert client.execute(
        f"SELECT answer_text,elapsed_ms,challenge_runs_json FROM {RESULT_TABLE} FINAL WHERE result_id=%(id)s",
        {"id": archived},
    ) == [("Archived åäö https://example.se", 123, '[{"state": "solved"}]')]
    # Replaying the cutover must not overwrite diagnostics from later direct writes.
    new = record("99")
    new["elapsed_ms"] = 777
    insert_results(client, [new])
    assert migrate_results(queue, client, batch_size=100) == {
        "postgres_results_verified": 2,
        "clickhouse_results": 3,
    }
    assert client.execute(
        f"SELECT elapsed_ms FROM {RESULT_TABLE} FINAL WHERE company_id='99'"
    ) == [(777,)]
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT payload ? 'answer_text' AS retained FROM processing.results WHERE result_id=%s",
            (unpublished,),
        )
        assert cursor.fetchone()["retained"] is True


def test_retired_task_cannot_resume_saved_execution_but_can_publish(store, clickhouse, brave_api):
    queue, dsn = store
    client, resource = clickhouse
    inputs(client, 1)
    fixture = brave_api()
    fixture.responder = lambda _: {}
    task = str(uuid4())
    with dg.DagsterInstance.ephemeral() as instance:
        original = materialize(resource, dsn, fixture, instance, task_id=task,
                               input_relation="corpscout.brave_test_input")
        assert original.success
        with queue.transaction() as cursor:
            cursor.execute("UPDATE processing.tasks SET status='cancelled' WHERE task_id=%s", (task,))
        client.execute("DROP TABLE corpscout.brave_test_input")
        resumed = materialize(resource, dsn, fixture, instance, execution_id=original.run_id)
        assert not resumed.success
        assert "inputs have been retired" in resumed.failure_data_for_node("company_brave_search_results").error.to_string()
        assert materialize(resource, dsn, fixture, instance, execution_id=original.run_id, mode="publish").success
        assert len(fixture.queries) == 1
