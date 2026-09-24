"""Draft queue constraints and legacy compatibility against disposable PostgreSQL."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from uuid import uuid4

import psycopg2
import pytest

from tests.test_processing_store import (
    MIGRATION,
    processing_postgres_url as processing_postgres_url,
    store as store,
)

DRAFT_SQL = """INSERT INTO processing.tasks
    (task_id,processor,config,work_config,status,queue_scope)
    VALUES (%s,%s,'{}','{}','draft',%s)"""


def test_only_one_concurrent_draft_per_processor_and_scope(store):
    queue, dsn = store

    def create_draft(_: int) -> str:
        with closing(psycopg2.connect(dsn)) as connection:
            try:
                with connection, connection.cursor() as cursor:
                    cursor.execute(DRAFT_SQL, (str(uuid4()), "webtech-v1", "admin"))
                return "created"
            except psycopg2.errors.UniqueViolation:
                return "existing"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(create_draft, range(2))) == ["created", "existing"]
    with queue.transaction() as cursor:
        cursor.execute(DRAFT_SQL, (str(uuid4()), "brave-v2", "admin"))
        cursor.execute(DRAFT_SQL, (str(uuid4()), "webtech-v1", "another-workspace"))
        cursor.execute("SELECT count(*) FROM processing.tasks WHERE status='draft'")
        assert cursor.fetchone()["count"] == 3


def test_freezing_task_allows_next_draft_and_cleanup_requires_completion(store):
    queue, _ = store
    task = str(uuid4())
    with queue.transaction() as cursor:
        cursor.execute(DRAFT_SQL, (task, "webtech-v1", "admin"))
        cursor.execute(
            "UPDATE processing.tasks SET status='selected',frozen_at=now() WHERE task_id=%s",
            (task,),
        )
        cursor.execute(DRAFT_SQL, (str(uuid4()), "webtech-v1", "admin"))
    with pytest.raises(psycopg2.errors.CheckViolation), queue.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=now() WHERE task_id=%s", (task,)
        )
    with queue.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET status='completed',completed_at=now() WHERE task_id=%s",
            (task,),
        )
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=now() WHERE task_id=%s", (task,)
        )
    assert queue.task(task)["inputs_purged_at"] is not None


@pytest.mark.parametrize("assignment", [
    "queue_scope=NULL",
    "queue_scope=' '",
    "status='selected'",
    "status='ready'",
    "status='completed',completed_at=now()",
    "frozen_at=now()",
])
def test_draft_cannot_be_unscoped_or_activated_without_freezing(store, assignment):
    queue, _ = store
    task = str(uuid4())
    with queue.transaction() as cursor:
        cursor.execute(DRAFT_SQL, (task, "webtech-v1", "admin"))
    with pytest.raises(psycopg2.errors.CheckViolation), queue.transaction() as cursor:
        cursor.execute(f"UPDATE processing.tasks SET {assignment} WHERE task_id=%s", (task,))


def test_import_receipts_keep_separate_sources_and_do_not_duplicate_ids(store):
    queue, _ = store
    task, first, second = (str(uuid4()) for _ in range(3))
    sql = """INSERT INTO processing.input_submissions
        (submission_id,task_id,source_name,selection_config,selection_fingerprint)
        VALUES (%s,%s,%s,'{}',%s)"""
    with queue.transaction() as cursor:
        cursor.execute(DRAFT_SQL, (task, "webtech-v1", "admin"))
        cursor.execute(sql, (first, task, "commoncrawl", "a" * 64))
        cursor.execute(sql, (second, task, "manual", "b" * 64))
    with pytest.raises(psycopg2.errors.UniqueViolation), queue.transaction() as cursor:
        cursor.execute(sql, (first, task, "commoncrawl", "a" * 64))
    with pytest.raises(psycopg2.errors.CheckViolation), queue.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.input_submissions SET status='completed' WHERE submission_id=%s",
            (first,),
        )
    with queue.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.input_submissions
            SET status='completed',finished_at=now(),input_count=10 WHERE submission_id=%s""",
            (first,),
        )
        cursor.execute("SELECT count(*) FROM processing.input_submissions WHERE task_id=%s", (task,))
        assert cursor.fetchone()["count"] == 2


def test_migration_roundtrip_preserves_existing_task_states(store):
    queue, _ = store
    up = MIGRATION.with_name("000124_processing_draft_tasks.up.sql").read_text()
    down = MIGRATION.with_name("000124_processing_draft_tasks.down.sql").read_text()
    with queue.transaction() as cursor:
        cursor.execute(down)
        for status in ("preparing", "selected", "ready", "cancelled"):
            cursor.execute(
                """INSERT INTO processing.tasks (task_id,processor,config,work_config,status)
                VALUES (%s,'legacy','{"saved":true}','{}',%s)""",
                (str(uuid4()), status),
            )
        cursor.execute("SELECT * FROM processing.tasks ORDER BY task_id")
        before = cursor.fetchall()
        cursor.execute(up)
        cursor.execute("SELECT count(*) FROM processing.tasks WHERE queue_scope IS NOT NULL")
        assert cursor.fetchone()["count"] == 0
        cursor.execute(down)
        cursor.execute("SELECT * FROM processing.tasks ORDER BY task_id")
        assert cursor.fetchall() == before


def test_rollback_refuses_to_discard_adopted_tasks(store):
    queue, _ = store
    task = str(uuid4())
    with queue.transaction() as cursor:
        cursor.execute(DRAFT_SQL, (task, "webtech-v1", "admin"))
    with pytest.raises(psycopg2.errors.RaiseException, match="metadata is in use"), queue.transaction() as cursor:
        cursor.execute(MIGRATION.with_name("000124_processing_draft_tasks.down.sql").read_text())
    assert queue.task(task)["status"] == "draft"
