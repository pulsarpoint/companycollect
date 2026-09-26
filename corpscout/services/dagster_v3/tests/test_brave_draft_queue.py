"""Brave drafts, recovery and history at real ClickHouse/PostgreSQL boundaries."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4
import asyncio
import psycopg2

import dagster as dg
import pytest

from dagster_v3.defs.common import draft_queue, queue_execution
from dagster_v3.defs.company_domains.assets import BraveSearchConfig
from dagster_v3.defs.company_domains.queue_input import (
    BraveQueueInputConfig,
    load_draft,
)
from dagster_v3.defs.company_domains.queue_tables import INPUT_RELATION, PROCESSOR
from dagster_v3.defs.company_domains.queue_execution import (
    start_execution,
    remaining_inputs,
    finish_execution,
)
from dagster_v3.defs.company_domains.result_writer import BraveResultWriter
from dagster_v3.defs.company_domains.results import insert_results
from tests.test_brave_clickhouse_results import materialize, record
from tests.brave_batch_service import brave_batch_service as brave_batch_service
from tests.test_company_domains_brave import LLM, brave_api as brave_api
from tests.test_ip_enrichment_input import server as server
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"


@pytest.fixture
def database(server):
    client, resource = server
    for table in (
        "company_brave_queue_input",
        "company_brave_task_sources",
        "se_company_brave_search_results_latest_success",
        "company_brave_search_results_latest",
        "se_company_brave_success_queue_latest",
        "se_company_brave_search_successes",
        "company_brave_search_results",
        "brave_draft_source",
    ):
        client.execute(f"DROP TABLE IF EXISTS corpscout.{table}")
    projection = (
        (MIGRATIONS / "000414_corpscout_se_company_brave_domains.up.sql")
        .read_text(encoding="utf-8")
        .split("-- Full immutable")[0]
    )
    for sql in (
        projection,
        *(
            (MIGRATIONS / name).read_text(encoding="utf-8")
            for name in (
                "000428_corpscout_brave_search_outcomes.up.sql",
                "000453_corpscout_brave_draft_queue.up.sql",
                "000455_corpscout_brave_search_versions.up.sql",
            )
        ),
    ):
        for statement in sql.split(";"):
            if statement.strip():
                client.execute(statement)
    client.execute(
        "CREATE TABLE corpscout.brave_draft_source (company_id String, company_name String) ENGINE=MergeTree ORDER BY company_id"
    )
    return client, resource


def add(resource, store, **changes):
    config = BraveQueueInputConfig(
        **dict(
            source_relation="corpscout.brave_draft_source",
            country_code="SE",
            select_all=True,
            submission_id=str(uuid4()),
        )
        | changes
    )
    return load_draft(
        config=config, run_id=str(uuid4()), clickhouse=resource, store=store
    )


def start(resource, store, task_id, **changes):
    config = BraveSearchConfig(task_id=task_id, llm=LLM, **changes)
    with store.selection_lock(task_id):
        return start_execution(
            store,
            resource,
            task_id=task_id,
            config=config,
            supplied=changes,
            run_id=str(uuid4()),
        )


def test_append_replay_first_name_wins_and_freeze(database, store):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.brave_draft_source VALUES ('1','One'), ('2','Two')"
    )
    config = BraveQueueInputConfig(
        source_relation="corpscout.brave_draft_source",
        country_code="SE",
        select_all=True,
        submission_id=str(uuid4()),
    )
    first = load_draft(
        config=config, run_id=str(uuid4()), clickhouse=resource, store=processing
    )
    assert processing.task(first["task_id"])["status"] == "draft"
    client.execute("TRUNCATE TABLE corpscout.brave_draft_source")
    client.execute(
        "INSERT INTO corpscout.brave_draft_source VALUES ('1','New name'), ('3','Three')"
    )
    second = add(resource, processing)
    assert second["task_id"] == first["task_id"] and second["total"] == 3
    assert client.execute(
        f"SELECT company_name FROM {INPUT_RELATION} WHERE company_id='1'"
    ) == [("One",)]
    assert (
        load_draft(
            config=config, run_id=str(uuid4()), clickhouse=resource, store=processing
        )["input_count"]
        == 2
    )
    task = start(resource, processing, first["task_id"])
    assert task["status"] == "selected"
    assert start(resource, processing, first["task_id"])["config"] == task["config"]
    assert add(resource, processing)["task_id"] != first["task_id"]
    with pytest.raises(ValueError, match="different selection"):
        load_draft(
            config=config.model_copy(update={"source_name": "changed"}),
            run_id=str(uuid4()),
            clickhouse=resource,
            store=processing,
        )


def test_failed_import_replaces_only_its_submission(database, store, monkeypatch):
    client, resource = database
    processing, _ = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','One')")
    other = add(resource, processing)
    client.execute("TRUNCATE TABLE corpscout.brave_draft_source")
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('2','Two')")
    config = BraveQueueInputConfig(
        source_relation="corpscout.brave_draft_source",
        country_code="SE",
        select_all=True,
        submission_id=str(uuid4()),
    )
    with monkeypatch.context() as patch:
        patch.setattr(
            draft_queue,
            "finish_submission",
            lambda *a, **kw: (_ for _ in ()).throw(
                RuntimeError("lost acknowledgement")
            ),
        )
        with pytest.raises(RuntimeError):
            load_draft(
                config=config,
                run_id=str(uuid4()),
                clickhouse=resource,
                store=processing,
            )
    with pytest.raises(ValueError, match="outstanding imports"):
        start(resource, processing, other["task_id"])
    client.execute("TRUNCATE TABLE corpscout.brave_draft_source")
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('3','Three')")
    result = load_draft(
        config=config, run_id=str(uuid4()), clickhouse=resource, store=processing
    )
    assert result["total"] == 2
    assert client.execute(
        f"SELECT company_id FROM {INPUT_RELATION} ORDER BY company_id"
    ) == [("1",), ("3",)]


def test_large_filter_and_invalid_records(database, store):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.brave_draft_source SELECT toString(number), concat('Name ',toString(number)) FROM numbers(10001)"
    )
    client.execute(
        "INSERT INTO corpscout.brave_draft_source VALUES ('','Bad'),('bad','')"
    )
    result = add(
        resource,
        processing,
        filters={"company_id": [str(i) for i in range(10001)] + ["", "bad"]},
    )
    assert result["total"] == 10001 and result["invalid_source_rows"] == 2
    with pytest.raises(ValueError, match="Swedish"):
        add(resource, processing, queue_scope="DE", country_code="DE")


def test_freshness_failure_window_force_and_cleanup(database, store):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.brave_draft_source VALUES ('1','One'),('2','Two'),('3','Three')"
    )
    one = record("1", age=1)
    two = record("2", age=2)
    failure = record("2", status="error", age=1)
    insert_results(client, [one, two, failure])
    draft = add(resource, processing)
    task = start(resource, processing, draft["task_id"])
    assert [r["company_id"] for r in remaining_inputs(client, task, limit=10)] == [
        "2",
        "3",
    ]
    # A later execution's success cannot change a frozen freshness decision.
    later = record("2")
    later["completed_at"] = datetime.now(UTC) + timedelta(seconds=5)
    insert_results(client, [later])
    assert [r["company_id"] for r in remaining_inputs(client, task, limit=10)] == [
        "2",
        "3",
    ]
    for row in remaining_inputs(client, task, limit=10):
        saved = record(
            row["company_id"], status="error" if row["company_id"] == "2" else "success"
        )
        saved.update(
            task_id=UUID(task["task_id"]),
            execution_id=UUID(task["config"]["execution"]["execution_id"]),
            input_id=row["input_id"],
        )
        insert_results(client, [saved])
    done = finish_execution(processing, resource, task)
    assert (
        done["succeeded_count"],
        done["terminal_failed_count"],
        done["skipped_count"],
    ) == (1, 1, 1)
    queue_execution.purge_completed_inputs(
        processing,
        resource,
        task_id=task["task_id"],
        processor=PROCESSOR,
        relation=INPUT_RELATION,
        label="Brave",
    )
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]
    assert client.execute(
        "SELECT count() FROM corpscout.company_brave_task_sources FINAL"
    ) == [(3,)]
    queue_execution.purge_completed_inputs(
        processing,
        resource,
        task_id=task["task_id"],
        processor=PROCESSOR,
        relation=INPUT_RELATION,
        label="Brave",
    )
    fresh = add(resource, processing)
    forced = start(resource, processing, fresh["task_id"], force_rescan=True)
    assert len(remaining_inputs(client, forced, limit=10)) == 3


@pytest.mark.parametrize("flush_seconds", [0.0, 0.05])
def test_writer_batches_acknowledges_and_flushes_without_new_results(database, flush_seconds):
    client, resource = database
    with BraveResultWriter(resource, max_items=10, max_seconds=flush_seconds) as writer:
        with ThreadPoolExecutor(max_workers=4) as workers:
            list(workers.map(writer.save, [record(str(i)) for i in range(4)]))
        assert client.execute(
            "SELECT count() FROM corpscout.company_brave_search_results FINAL"
        ) == [(4,)]


def test_freshness_is_specific_to_search_and_revision(database, store):
    client, resource = database
    processing, _ = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','One'),('2','Two'),('3','Three')")
    search_id = str(uuid4())
    matching, older_version, another_search = [record(str(i), age=1) for i in range(1, 4)]
    for row, saved_id, revision in ((matching, search_id, 2), (older_version, search_id, 1), (another_search, str(uuid4()), 2)):
        row.update(search_id=saved_id, search_revision=revision, search_name="Website")
    insert_results(client, [matching, older_version, another_search])
    draft = add(resource, processing)
    task = start(resource, processing, draft["task_id"], search_id=search_id, search_name="Website", search_revision=2)
    assert [row["company_id"] for row in remaining_inputs(client, task, limit=10)] == ["2", "3"]
    assert start(resource, processing, draft["task_id"])["config"] == task["config"]
    with pytest.raises(ValueError, match="frozen"):
        start(resource, processing, draft["task_id"], search_revision=3)


def test_original_default_answers_match_seeded_search(database, store):
    client, resource = database
    processing, _ = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','One'),('2','Two')")
    original = record("1", age=1)
    original.update(company_name="One", query="Find the official website of One.")
    custom = record("2", age=1)
    columns = [name for name in original if not name.startswith("search_")]
    client.execute(f"INSERT INTO corpscout.company_brave_search_results ({','.join(columns)}) VALUES",
                   [tuple(row[name] for name in columns) for row in (original, custom)])
    task = start(resource, processing, add(resource, processing)["task_id"],
                 search_id="54d90187-85d5-45dc-9603-cd7c4a7d31d1", search_revision=1, search_name="Official website")
    assert [row["company_id"] for row in remaining_inputs(client, task, limit=10)] == ["2"]


def test_draft_end_to_end_preserves_errors_and_resumes_without_browser_calls(
    database, store, brave_batch_service, monkeypatch
):
    client, resource = database
    processing, dsn = store
    client.execute(
        "INSERT INTO corpscout.brave_draft_source VALUES ('1','One'),('2','Two')"
    )
    draft = add(resource, processing)
    fixture = brave_batch_service(resource,dsn,draft["task_id"])
    monkeypatch.setenv("LLM_CONTROL_PG_URL",dsn)
    fixture.responder = lambda p: (
        {"status": "error", "error_type": "NoAnswer", "error_stage": "answer"}
        if "Two" in p["query"]
        else {}
    )
    with dg.DagsterInstance.ephemeral() as instance:
        result = materialize(
            resource, dsn, fixture, instance, task_id=draft["task_id"], llm=fixture.llm,
            tags={"llm/request_id":fixture.owner,"processing/task_id":draft["task_id"]},
            search_id="54d90187-85d5-45dc-9603-cd7c4a7d31d1",
            search_name="Official website", search_revision=1,
        )
        assert result.success
        assert len(fixture.queries) == 2
        assert client.execute("SELECT DISTINCT search_name,search_revision FROM corpscout.company_brave_search_results") == [("Official website", 1)]
        assert processing.task(draft["task_id"])["status"] == "completed"
        assert processing.task(draft["task_id"])["terminal_failed_count"] == 1
        assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]
        assert materialize(
            resource, dsn, fixture, instance, task_id=draft["task_id"], llm=fixture.llm
        ).success
        assert len(fixture.queries) == 2


def test_frozen_profile_rejects_changes_but_allows_transport_settings(database, store):
    client, resource = database
    processing, _ = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','One')")
    task_id = add(resource, processing)["task_id"]
    original = start(
        resource, processing, task_id, query_template="Website of {company_name}"
    )
    resumed = start(resource, processing, task_id, input_batch_size=400)
    assert resumed["config"] == original["config"]
    for change in (
        {"force_rescan": True},
        {"recent_days": 3},
        {"query_template": "Changed"},
    ):
        with pytest.raises(ValueError, match="frozen"):
            start(resource, processing, task_id, **change)


def test_service_batches_wait_for_publication_and_recover_lost_write_ack(
    database,store,brave_batch_service,monkeypatch,
):
    client,resource = database
    processing,dsn = store
    client.execute("INSERT INTO corpscout.brave_draft_source SELECT toString(number),concat('Company ',toString(number)) FROM numbers(9)")
    task_id = add(resource,processing)["task_id"]
    fixture = brave_batch_service(resource,dsn,task_id)
    monkeypatch.setenv("LLM_CONTROL_PG_URL",dsn)
    publish = fixture.queue.publisher.publish
    completed_batches = []
    failed_ack = False

    async def publish_then_lose_ack(batch,records):
        nonlocal failed_ack
        await publish(batch,records)
        if not failed_ack:
            failed_ack = True
            raise OSError("lost acknowledgement after durable insert")
        completed_batches.append(str(batch.batch_id))

    fixture.queue.publisher.publish = publish_then_lose_ack
    ask = fixture.queue.ask

    async def require_previous_batch_published(payload):
        if len(fixture.batches) > 1:
            assert fixture.batches[-2]["batch_id"] in completed_batches
        return await ask(payload)

    fixture.queue.ask = require_previous_batch_published
    with dg.DagsterInstance.ephemeral() as instance:
        result = materialize(resource,dsn,fixture,instance,task_id=task_id,llm=fixture.llm,input_batch_size=4,
            tags={"llm/request_id":fixture.owner,"processing/task_id":task_id})
        assert result.success
    assert [len(batch["items"]) for batch in fixture.batches] == [4,4,1]
    assert len(fixture.queries) == 9
    assert client.execute("SELECT count(),uniqExact(result_id) FROM corpscout.company_brave_search_results") == [(9,9)]
    assert client.execute("SELECT count() FROM corpscout.se_company_brave_search_results_latest_success FINAL") == [(9,)]
    with psycopg2.connect(dsn) as connection,connection.cursor() as cursor:
        cursor.execute("SELECT state,count(*) FROM processing.llm_external_requests GROUP BY state")
        assert cursor.fetchall() == [("completed",9)]
        cursor.execute("UPDATE processing.llm_external_requests SET state='submitted'")
    # Reconcile a lost PostgreSQL completion acknowledgement using Brave's status field.
    from dagster_v3.defs.common.llm_monitor import reconcile_external
    monkeypatch.setenv("BROWSER_API_URL",fixture.config["api_url"])
    monkeypatch.setenv("BROWSER_API_TOKEN",fixture.config["api_token"])
    assert reconcile_external() == 9
    with psycopg2.connect(dsn) as connection,connection.cursor() as cursor:
        cursor.execute("SELECT state,count(*) FROM processing.llm_external_requests GROUP BY state")
        assert cursor.fetchall() == [("completed",9)]


def test_unfinished_service_batch_resumes_with_new_backoffice_owner(
    database,store,brave_batch_service,monkeypatch,
):
    client,resource = database
    processing,dsn = store
    client.execute("INSERT INTO corpscout.brave_draft_source SELECT toString(number),concat('Company ',toString(number)) FROM numbers(4)")
    task_id = add(resource,processing)["task_id"]
    fixture = brave_batch_service(resource,dsn,task_id)
    monkeypatch.setenv("LLM_CONTROL_PG_URL",dsn)
    ask = fixture.queue.ask

    async def interrupt_one(payload):
        if payload.query == "Find the official website of Company 3.":
            await asyncio.sleep(.2)
            raise RuntimeError("interrupted before saving response")
        return await ask(payload)

    fixture.queue.ask = interrupt_one
    with dg.DagsterInstance.ephemeral() as instance:
        failed = materialize(resource,dsn,fixture,instance,task_id=task_id,llm=fixture.llm,
            tags={"llm/request_id":fixture.owner,"processing/task_id":task_id})
        assert not failed.success
        assert len(fixture.queries) == 3
        assert client.execute("SELECT count() FROM corpscout.company_brave_search_results") == [(0,)]
        assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(4,)]
        owner = str(uuid4())
        with psycopg2.connect(dsn) as connection,connection.cursor() as cursor:
            cursor.execute("UPDATE processing.run_requests SET finished_at=now(),status='failed'")
            cursor.execute("INSERT INTO processing.run_requests(request_id,task_id,job_name) VALUES (%s,%s,'__ephemeral_asset_job__')",(owner,task_id))
            cursor.execute("INSERT INTO processing.run_llm_dependencies VALUES (%s,%s,1,'browser')",(owner,fixture.llm["profile_id"]))
        fixture.queue.ask = ask
        resumed = materialize(resource,dsn,fixture,instance,task_id=task_id,llm=fixture.llm,
            tags={"llm/request_id":owner,"processing/task_id":task_id})
        assert resumed.success
        assert len(fixture.batches) == 1
        assert len(fixture.queries) == 4
        assert client.execute("SELECT count(),uniqExact(result_id) FROM corpscout.company_brave_search_results") == [(4,4)]


def test_writer_failure_releases_waiting_callbacks_and_keeps_inputs(
    database, store, monkeypatch
):
    from dagster_v3.defs.company_domains import result_writer

    client, resource = database
    processing, _ = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','One')")
    task_id = add(resource, processing)["task_id"]
    start(resource, processing, task_id)

    def fail(*args):
        raise OSError("storage unavailable")

    monkeypatch.setattr(result_writer, "insert_results", fail)
    with pytest.raises(RuntimeError, match="publication failed"):
        with BraveResultWriter(resource, max_items=2, max_seconds=0.05) as writer:
            with ThreadPoolExecutor(max_workers=4) as workers:
                futures = [
                    workers.submit(writer.save, record(str(i))) for i in range(4)
                ]
                for future in futures:
                    with pytest.raises(RuntimeError, match="publication failed"):
                        future.result(timeout=2)
    assert processing.task(task_id)["status"] == "selected"
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(1,)]


def test_cleanup_failure_keeps_membership_and_is_resumable(
    database, store, monkeypatch
):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.brave_draft_source VALUES ('1','One'),('1','')"
    )
    task_id = add(resource, processing)["task_id"]
    start(resource, processing, task_id)
    queue_execution.record_completion(
        processing, task_id=task_id, remaining=0, succeeded=0, failed=0
    )
    execute = type(client).execute

    def drop_fails(self, sql, *args, **kwargs):
        if "DROP PARTITION" in sql:
            raise OSError("drop interrupted")
        return execute(self, sql, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "execute", drop_fails)
        with pytest.raises(OSError, match="interrupted"):
            queue_execution.purge_completed_inputs(
                processing,
                resource,
                task_id=task_id,
                processor=PROCESSOR,
                relation=INPUT_RELATION,
            )
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(1,)]
    assert client.execute(
        "SELECT company_name FROM corpscout.company_brave_task_sources FINAL"
    ) == [("One",)]
    assert processing.task(task_id)["inputs_purged_at"] is None
    queue_execution.purge_completed_inputs(
        processing,
        resource,
        task_id=task_id,
        processor=PROCESSOR,
        relation=INPUT_RELATION,
    )
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]
    assert client.execute(
        "SELECT count() FROM corpscout.company_brave_task_sources FINAL"
    ) == [(1,)]


@pytest.mark.parametrize("values", [
    {},
    {"filters": {"company_name": []}},
    {"filters": {"company_name; DROP TABLE x": ["active"]}},
    {"company_ids": ["1"], "company_name_column": "company_name)"},
    {"select_all": True, "source_relation": INPUT_RELATION},
    {"select_all": True, "source_relation": "corpscout.company_brave_search_input"},
])
def test_input_requires_explicit_selection_and_safe_source(values):
    with pytest.raises(ValueError):
        BraveQueueInputConfig(**{
            "source_relation": "corpscout.brave_draft_source",
            "country_code": "SE", "submission_id": str(uuid4()), **values,
        })


def test_selection_combines_name_id_length_and_exclusions(database, store):
    client, resource = database
    processing, _ = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('10','Alpha AB'), ('20','Alpha excluded'), ('30','Beta AB'), ('400','Alpha long')")
    task = add(resource, processing, company_name_pattern="Alpha%", company_id_length=2,
               excluded_company_ids=["20"])
    assert task["total"] == 1
    assert client.execute(f"SELECT company_id FROM {INPUT_RELATION}") == [("10",)]


def test_retirement_drops_only_legacy_inputs(database, store):
    client, resource = database
    processing, _ = store
    migration = MIGRATIONS / "000454_corpscout_retire_brave_legacy_input.up.sql"
    for statement in migration.with_name(migration.name.replace(".up.", ".down.")).read_text().split(";"):
        if statement.strip():
            client.execute(statement)
    client.execute("INSERT INTO corpscout.company_brave_search_input VALUES ('SE:old','old','Old AB','SE','')")
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','One')")
    add(resource, processing)
    client.execute("INSERT INTO corpscout.company_brave_task_sources SELECT *,now64(6) FROM corpscout.company_brave_queue_input")
    insert_results(client, [record("1")])
    retained = [INPUT_RELATION, "corpscout.company_brave_task_sources", "corpscout.company_brave_search_results"]
    before = {table: client.execute(f"SELECT * FROM {table}") for table in retained}
    for _ in range(2):
        for statement in migration.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
    assert client.execute("EXISTS TABLE corpscout.company_brave_search_input") == [(0,)]
    assert {table: client.execute(f"SELECT * FROM {table}") for table in retained} == before
