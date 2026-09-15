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
        yield ProcessingStore(connection), dsn
    finally:
        connection.close()
        with admin.cursor() as cursor:
            cursor.execute(f"DROP DATABASE {database} WITH (FORCE)")
        admin.close()


def freeze(
    store, names=("First AB", "Second AB"), task_id=None, template="Find {company_name}"
):
    task_id = task_id or str(uuid4())
    store.freeze(
        task_id,
        processor="brave-v2",
        config={"query_template": template},
        work_config={},
        inputs=(
            {"input_id": str(i), "company_name": name} for i, name in enumerate(names)
        ),
        query_template=template,
        freshness_days=30,
    )
    return task_id


def claim(store, task):
    return store.claim(task, owner="run-1", lease_seconds=300, max_attempts=3)


def complete(store, item, status="success", answer="Copied response"):
    return store.complete(
        item,
        status=status,
        payload={"answer_text": answer},
        completed_at=datetime.now(UTC),
        max_attempts=3,
        retry_seconds=0,
    )


def test_snapshot_is_frozen_and_resume_never_consumes_new_input(store):
    queue, _ = store
    task = freeze(queue)

    def unexpected_input():
        pytest.fail("resume reran the input selection")
        yield

    queue.freeze(
        task,
        processor="brave-v2",
        config={"query_template": "Find {company_name}"},
        work_config={},
        inputs=unexpected_input(),
        query_template="Find {company_name}",
        freshness_days=30,
    )
    first = claim(queue, task)
    assert first.input_data == {"input_id": "0", "company_name": "First AB"}
    assert first.query == "Find First AB"
    assert queue.progress(task)["total"] == 2
    with pytest.raises(ValueError, match="configuration"):
        freeze(queue, task_id=task, template="Different {company_name}")


def test_bad_or_duplicate_inputs_leave_no_partial_task(store):
    queue, _ = store
    for inputs, template in [
        (
            [
                {"input_id": "1", "company_name": "A"},
                {"input_id": "1", "company_name": "B"},
            ],
            "Find {company_name}",
        ),
        ([{"input_id": "", "company_name": "A"}], "Find {company_name}"),
        ([{"input_id": "1", "company_name": "A"}], "Find {missing}"),
        ([{"input_id": "1", "company_name": None}], "Find {company_name}"),
    ]:
        task = str(uuid4())
        with pytest.raises((ValueError, psycopg2.IntegrityError)):
            queue.freeze(
                task,
                processor="brave-v2",
                config={},
                work_config={},
                inputs=iter(inputs),
                query_template=template,
                freshness_days=0,
            )
        assert queue.task(task) is None
    empty = freeze(queue, names=())
    assert queue.progress(empty)["remaining"] == 0


def test_out_of_order_completion_and_restart_never_skip_slow_item(store):
    queue, dsn = store
    task = freeze(queue)
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
        restarted.release("run-1")
        recovered = claim(restarted, task)
        assert recovered.input_id == slow.input_id
        assert recovered.lease_token != slow.lease_token
        assert complete(restarted, slow) is None
        assert complete(restarted, recovered) is not None
        assert restarted.progress(task)["remaining"] == 0
        assert claim(restarted, task) is None


def test_independent_connections_claim_distinct_items(store):
    queue, dsn = store
    task = freeze(queue, names=tuple(f"Company {i}" for i in range(20)))
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
    task = freeze(queue, names=("A",))
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
    task = freeze(queue)
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
    assert queue.progress(task)["unpublished"] == 0


def test_cache_keys_include_input_template_and_query_type(store):
    queue, _ = store
    first = freeze(queue, names=("A",))
    complete(queue, claim(queue, first))
    queue.acknowledge(
        queue.export_batch(first, limit=100, destination="company_brave_info_v1")
    )
    cached = freeze(queue, names=("A",))
    assert queue.progress(cached)["skipped"] == 1
    assert claim(queue, cached) is None
    changed = freeze(queue, names=("A",), template="New {company_name}")
    assert queue.progress(changed)["queued"] == 1
    renamed = freeze(queue, names=("Renamed",))
    assert queue.progress(renamed)["queued"] == 1


def test_selection_limits_do_not_change_semantic_work_identity(store):
    queue, _ = store
    first, second = str(uuid4()), str(uuid4())
    for task, limit in [(first, 1), (second, 10)]:
        queue.freeze(
            task,
            processor="brave-v2",
            config={"limit": limit},
            work_config={"query_type": "website", "input_namespace": "companies"},
            inputs=[{"input_id": "1", "company_name": "A"}],
            query_template="Find {company_name}",
            freshness_days=30,
        )
        if task == first:
            complete(queue, claim(queue, task))
            queue.acknowledge(
                queue.export_batch(task, limit=100, destination="company_brave_info_v1")
            )
    assert queue.progress(second)["skipped"] == 1


def test_duplicate_after_a_snapshot_batch_rolls_back_the_entire_snapshot(store):
    queue, _ = store
    task = str(uuid4())
    rows = [{"input_id": str(i), "company_name": f"Name {i}"} for i in range(1000)]
    rows.append({"input_id": "0", "company_name": "Duplicate"})
    with pytest.raises(psycopg2.IntegrityError):
        queue.freeze(
            task,
            processor="brave-v2",
            config={},
            work_config={},
            inputs=rows,
            query_template="Find {company_name}",
            freshness_days=0,
        )
    assert queue.task(task) is None


def test_heartbeat_only_renews_live_claims(store):
    queue, _ = store
    task = freeze(queue)
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
