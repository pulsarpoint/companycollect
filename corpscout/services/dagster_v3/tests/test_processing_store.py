"""Recovery and concurrency at the real PostgreSQL boundary."""

import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

MIGRATION = (
    Path(__file__).parents[3] / "database/migrations/000119_processing_tasks.up.sql"
)


@pytest.fixture(scope="session")
def processing_postgres_url():
    name = "processing-postgres-test-" + uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            "127.0.0.1::5432",
            "-e",
            "POSTGRES_USER=processing_test",
            "-e",
            "POSTGRES_PASSWORD=processing_test",
            "-e",
            "POSTGRES_DB=processing_test",
            "postgres:17",
        ],
        check=True,
        capture_output=True,
    )
    try:
        port = int(
            subprocess.check_output(["docker", "port", name, "5432"], text=True)
            .strip()
            .rsplit(":", 1)[1]
        )
        url = f"postgresql://processing_test:processing_test@127.0.0.1:{port}/processing_test"
        deadline = time.monotonic() + 30
        while True:
            try:
                psycopg2.connect(url, connect_timeout=1).close()
                break
            except psycopg2.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


@pytest.fixture
def store(processing_postgres_url):
    from dagster_v3.defs.common.processing import ProcessingStore

    # Both server and database are disposable, never the checkout's production URL.
    admin = psycopg2.connect(processing_postgres_url)
    admin.autocommit = True
    database = "processing_test_" + uuid4().hex
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {database}")
    dsn = processing_postgres_url.rsplit("/", 1)[0] + "/" + database
    connection = psycopg2.connect(dsn)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(MIGRATION.read_text())
            cursor.execute(
                MIGRATION.with_name(
                    "000120_processing_clickhouse_input.up.sql"
                ).read_text()
            )
            cursor.execute(
                MIGRATION.with_name(
                    "000121_processing_brave_input_relation.up.sql"
                ).read_text()
            )
        yield ProcessingStore(connection), dsn
    finally:
        connection.close()
        with admin.cursor() as cursor:
            cursor.execute(f"DROP DATABASE {database} WITH (FORCE)")
        admin.close()


def prepare_task(
    store, names=("First AB", "Second AB"), task_id=None, template="Find {company_name}"
):
    task_id = task_id or str(uuid4())
    inputs = {
        str(i): {"input_id": str(i), "company_name": name}
        for i, name in enumerate(names)
    }
    store.register(
        task_id,
        processor="brave-v2",
        config={"query_template": template},
        work_config={},
        source_info={"total": len(inputs), "upper_id": max(inputs, default="")},
    )
    if inputs:
        store.admit(task_id, after=None, input_ids=sorted(inputs), capacity=len(inputs))
    return task_id


def claim(store, task):
    return store.claim(task, owner="run-1", lease_seconds=300, max_attempts=3)


def complete(store, item, status="success", answer="Copied response"):
    return store.complete(
        item,
        status=status,
        work_key="test-work-" + item.input_id,
        payload={"answer_text": answer, "query": "Find " + item.input_id},
        completed_at=datetime.now(UTC),
        max_attempts=3,
        retry_seconds=0,
    )


def test_registration_keeps_millions_of_inputs_out_of_postgres(store):
    queue, _ = store
    task = str(uuid4())
    source = {"total": 3_000_000, "upper_id": "999999"}
    queue.register(
        task, processor="brave-v2", config={}, work_config={}, source_info=source
    )
    queue.register(
        task, processor="brave-v2", config={}, work_config={}, source_info=source
    )
    assert (
        queue.progress(task)["total"] == queue.progress(task)["remaining"] == 3_000_000
    )
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) FROM processing.items")
        assert cursor.fetchone()["count"] == 0
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='processing' AND table_name='items'"
        )
        assert not {"input_data", "query", "work_key"} & {
            row["column_name"] for row in cursor.fetchall()
        }
    with pytest.raises(ValueError, match="configuration"):
        queue.register(
            task,
            processor="brave-v2",
            config={"changed": True},
            work_config={},
            source_info=source,
        )


def test_admission_cursor_and_ids_commit_together_and_limit_open_work(store):
    queue, dsn = store
    task = str(uuid4())
    queue.register(
        task,
        processor="brave-v2",
        config={},
        work_config={},
        source_info={"total": 10, "upper_id": "9"},
    )
    assert queue.admit(task, after=None, input_ids=["0", "1"], capacity=2)
    assert not queue.admit(task, after=None, input_ids=["0", "1"], capacity=2)
    assert not queue.admit(task, after="1", input_ids=["2"], capacity=2)
    assert queue.task(task)["source_cursor"] == "1"
    from dagster_v3.defs.common.processing import ProcessingStore

    with closing(psycopg2.connect(dsn)) as connection:
        recovered = ProcessingStore(connection)
        assert recovered.task(task)["admitted_count"] == 2
        complete(recovered, claim(recovered, task))
        assert recovered.admit(task, after="1", input_ids=["2"], capacity=2)
    assert queue.progress(task)["remaining"] == 9
    assert queue.progress(task)["queued"] == 9
    for values in (["3", "3"], ["4", "3"], [""], ["bad\x00id"]):
        with pytest.raises(ValueError):
            queue.admit(task, after="2", input_ids=values, capacity=10)
        assert queue.task(task)["source_cursor"] == "2"


def test_failed_admission_rolls_back_inserted_ids_and_cursor(store):
    queue, _ = store
    task = str(uuid4())
    queue.register(
        task,
        processor="brave-v2",
        config={},
        work_config={},
        source_info={"total": 10, "upper_id": "9"},
    )
    with queue.transaction() as cursor:
        cursor.execute(
            "INSERT INTO processing.items(task_id,input_id) VALUES (%s,'1')", (task,)
        )
    with pytest.raises(psycopg2.IntegrityError):
        queue.admit(task, after=None, input_ids=["0", "1"], capacity=10)
    assert queue.task(task)["source_cursor"] is None
    assert queue.task(task)["admitted_count"] == 0
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT input_id FROM processing.items WHERE task_id=%s", (task,)
        )
        assert [row["input_id"] for row in cursor.fetchall()] == ["1"]


def test_out_of_order_completion_and_restart_never_skip_slow_item(store):
    queue, dsn = store
    task = prepare_task(queue)
    slow, fast = claim(queue, task), claim(queue, task)
    result = complete(queue, fast)
    assert result is not None
    counts = queue.progress(task)
    assert {
        k: counts[k]
        for k in ("total", "running", "succeeded", "remaining", "unpublished")
    } == {"total": 2, "running": 1, "succeeded": 1, "remaining": 1, "unpublished": 1}
    from dagster_v3.defs.common.processing import ProcessingStore

    with closing(psycopg2.connect(dsn)) as connection:
        restarted = ProcessingStore(connection)
        restarted.release("run-1", max_attempts=3)
        recovered = claim(restarted, task)
        assert recovered.input_id == slow.input_id
        assert recovered.lease_token != slow.lease_token
        assert complete(restarted, slow) is None
        assert complete(restarted, recovered) is not None
        assert restarted.progress(task)["remaining"] == 0
        assert claim(restarted, task) is None


def test_independent_connections_claim_distinct_items(store):
    queue, dsn = store
    task = prepare_task(queue, names=tuple(f"Company {i}" for i in range(20)))
    from dagster_v3.defs.common.processing import ProcessingStore

    def work(_):
        connection = psycopg2.connect(dsn)
        try:
            worker = ProcessingStore(connection)
            items = []
            while item := claim(worker, task):
                items.append(item.input_id)
                complete(worker, item)
            return items
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        claimed = sum(executor.map(work, range(4)), [])
    assert len(claimed) == len(set(claimed)) == 20
    assert queue.progress(task)["succeeded"] == 20


def test_retry_budget_and_expired_tokens(store):
    queue, _ = store
    task = prepare_task(queue, names=("A",))
    old = claim(queue, task)
    with queue.connection, queue.connection.cursor() as cursor:
        cursor.execute(
            "UPDATE processing.items SET lease_expires_at=now()-interval '1 second' WHERE task_id=%s",
            (task,),
        )
    replacement = claim(queue, task)
    assert replacement.attempt == 2
    assert complete(queue, old) is None
    assert complete(queue, replacement, status="error", answer="") is not None
    last = claim(queue, task)
    assert last.attempt == 3
    complete(queue, last, status="error", answer="")
    assert claim(queue, task) is None
    assert queue.progress(task)["terminal_failed"] == 1
    assert queue.progress(task)["remaining"] == 0


def test_batches_are_closed_replayable_and_do_not_capture_later_results(store):
    queue, _ = store
    task = prepare_task(queue)
    first, second = claim(queue, task), claim(queue, task)
    result_id = complete(queue, first)
    batch = queue.export_batch(task, limit=100, destination="company_brave_info_v1")
    complete(queue, second)
    assert (
        queue.export_batch(task, limit=100, destination="company_brave_info_v1")
        == batch
    )
    assert batch.result_count == 1
    queue.acknowledge(batch)
    assert queue.progress(task)["unpublished"] == 1
    next_batch = queue.export_batch(
        task, limit=100, destination="company_brave_info_v1"
    )
    assert next_batch.batch_id != batch.batch_id
    with queue.connection, queue.connection.cursor() as cursor:
        cursor.execute(
            "SELECT result_id::text FROM processing.results WHERE export_batch_id=%s",
            (batch.batch_id,),
        )
        assert cursor.fetchall() == [(result_id,)]
    queue.acknowledge(next_batch)
    queue.acknowledge(next_batch)
    assert queue.progress(task)["unpublished"] == 0


def test_freshness_skips_only_published_matching_results_with_a_live_claim(store):
    queue, _ = store
    first = prepare_task(queue, names=("A",))
    complete(queue, claim(queue, first))
    next_task = prepare_task(queue, names=("A",))
    item = claim(queue, next_task)
    assert not queue.skip_if_fresh(item, work_key="test-work-0", freshness_days=30)
    queue.acknowledge(
        queue.export_batch(first, limit=100, destination="company_brave_info_v1")
    )
    assert not queue.skip_if_fresh(item, work_key="changed-query", freshness_days=30)
    assert not queue.skip_if_fresh(item, work_key="test-work-0", freshness_days=0)
    assert queue.skip_if_fresh(item, work_key="test-work-0", freshness_days=30)
    assert not queue.skip_if_fresh(item, work_key="test-work-0", freshness_days=30)
    assert queue.progress(next_task)["skipped"] == 1
    assert queue.progress(next_task)["remaining"] == 0
    assert complete(queue, item) is None


def test_work_identity_changes_with_input_template_and_query_type():
    from dagster_v3.defs.common.processing import work_key

    values = {"input_id": "1", "company_name": "A"}
    key = work_key(
        "brave-v2", {"query_type": "website"}, "Find {company_name}", values, "Find A"
    )
    assert key != work_key(
        "brave-v2", {"query_type": "other"}, "Find {company_name}", values, "Find A"
    )
    assert key != work_key(
        "brave-v2", {"query_type": "website"}, "Find {company_name}.", values, "Find A."
    )
    assert key != work_key(
        "brave-v2",
        {"query_type": "website"},
        "Find {company_name}",
        {**values, "company_name": "B"},
        "Find B",
    )


def test_heartbeat_only_renews_live_claims(store):
    queue, _ = store
    task = prepare_task(queue)
    expired, live = claim(queue, task), claim(queue, task)
    with queue.connection, queue.connection.cursor() as cursor:
        cursor.execute(
            "UPDATE processing.items SET lease_expires_at=now()-interval '1 second' WHERE input_id=%s",
            (expired.input_id,),
        )
        cursor.execute(
            "UPDATE processing.items SET lease_expires_at=now()+interval '1 second' WHERE input_id=%s",
            (live.input_id,),
        )
    queue.heartbeat("run-1", lease_seconds=300)
    assert complete(queue, expired) is None
    assert complete(queue, live) is not None
    assert claim(queue, task).input_id == expired.input_id


@pytest.mark.parametrize(
    "template",
    [
        "{company_name.__class__}",
        "{company_name!r}",
        "{company_name:>20}",
        "{missing}",
        "   ",
    ],
)
def test_templates_reject_missing_or_non_column_expressions(template):
    from dagster_v3.defs.common.processing import render_query

    with pytest.raises(ValueError):
        render_query(template, {"company_name": "A"})


def test_abandoned_last_attempt_is_terminal_and_does_not_block_the_next_item(store):
    queue, _ = store
    task = prepare_task(queue)
    exhausted = claim(queue, task)
    with queue.connection, queue.connection.cursor() as cursor:
        cursor.execute(
            "UPDATE processing.items SET attempt=3 WHERE task_id=%s AND input_id=%s",
            (task, exhausted.input_id),
        )
    queue.release("run-1", max_attempts=3)
    next_item = claim(queue, task)
    assert next_item.input_id == "1"
    assert queue.progress(task)["terminal_failed"] == 1
    assert complete(queue, exhausted) is None


def test_migration_preserves_legacy_responses_and_refuses_unfinished_inputs(store):
    queue, _ = store
    task, result = str(uuid4()), str(uuid4())
    with queue.transaction() as cursor:
        cursor.execute("DROP SCHEMA processing CASCADE")
        cursor.execute(MIGRATION.read_text())
        cursor.execute(
            "INSERT INTO processing.tasks(task_id,processor,config,work_config,status,total) VALUES (%s,'brave-v2','{}','{}','ready',1)",
            (task,),
        )
        cursor.execute(
            "INSERT INTO processing.items(task_id,input_id,input_data,query,work_key) VALUES (%s,'1','{\"company_name\":\"Original AB\",\"country_code\":\"SE\"}','Find Original AB','old-key')",
            (task,),
        )
    with pytest.raises(psycopg2.errors.RaiseException, match="Finish legacy"):
        with queue.transaction() as cursor:
            cursor.execute(
                MIGRATION.with_name(
                    "000120_processing_clickhouse_input.up.sql"
                ).read_text()
            )
    with queue.transaction() as cursor:
        cursor.execute(
            "INSERT INTO processing.results(result_id,task_id,input_id,work_key,attempt,status,payload,completed_at) VALUES (%s,%s,'1','old-key',1,'success','{\"answer_text\":\"Exact copied answer åäö\"}',now())",
            (result, task),
        )
        cursor.execute(
            "UPDATE processing.items SET state='succeeded',attempt=1,accepted_result_id=%s WHERE task_id=%s",
            (result, task),
        )
        cursor.execute("SELECT * FROM processing.brave_export")
        before = dict(cursor.fetchone())
        cursor.execute(
            MIGRATION.with_name("000120_processing_clickhouse_input.up.sql").read_text()
        )
        cursor.execute("SELECT * FROM processing.brave_export")
        assert dict(cursor.fetchone()) == before
    assert queue.progress(task)["succeeded"] == 1
    assert queue.progress(task)["remaining"] == 0
    assert queue.progress(task)["unpublished"] == 1
    queue.acknowledge(
        queue.export_batch(task, limit=100, destination="company_brave_info_v1")
    )
    assert queue.progress(task)["unpublished"] == 0
