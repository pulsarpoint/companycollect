"""Draft task and import receipts; bulk inputs live in the processor's ClickHouse table."""

from uuid import uuid4

from psycopg2.extras import Json

from dagster_v3.defs.common.processing import ProcessingStore


def find_draft(
    store: ProcessingStore, *, scope: str, processor: str, task_id: str | None
) -> str:
    with store.transaction() as cursor:
        # Serializes find-or-create even when no draft row exists yet.
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"draft:{processor}:{scope}",),
        )
        if task_id is not None:
            cursor.execute(
                "SELECT * FROM processing.tasks WHERE task_id=%s", (task_id,)
            )
            task = cursor.fetchone()
            if task is not None:
                if (
                    task["queue_scope"] != scope
                    or task["processor"] != processor
                    or task["status"] != "draft"
                ):
                    raise ValueError(
                        "task_id must identify an open draft in this scope"
                    )
                return str(task["task_id"])
        cursor.execute(
            "SELECT task_id FROM processing.tasks WHERE queue_scope=%s AND processor=%s AND status='draft'",
            (scope, processor),
        )
        draft = cursor.fetchone()
        if draft is not None:
            if task_id is not None and str(draft["task_id"]) != task_id:
                raise ValueError(
                    "This scope already has an open draft; use its task_id"
                )
            return str(draft["task_id"])
        identity = task_id or str(uuid4())
        cursor.execute(
            "INSERT INTO processing.tasks (task_id,processor,queue_scope,config,work_config,status) VALUES (%s,%s,%s,'{}','{}','draft')",
            (identity, processor, scope),
        )
        return identity


def submission(store: ProcessingStore, submission_id: str) -> dict | None:
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT * FROM processing.input_submissions WHERE submission_id=%s",
            (submission_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row is not None else None


def prepare_submission(
    store: ProcessingStore,
    *,
    task_id: str,
    submission_id: str,
    source: str,
    selection: dict,
    fingerprint: str,
) -> dict:
    """Caller holds the task's selection_lock through all external writes."""
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT status FROM processing.tasks WHERE task_id=%s FOR UPDATE",
            (task_id,),
        )
        if cursor.fetchone()["status"] != "draft":
            raise ValueError("Queue is frozen; add inputs to the next draft")
        cursor.execute(
            """INSERT INTO processing.input_submissions
            (submission_id,task_id,source_name,selection_config,selection_fingerprint)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT (submission_id) DO NOTHING""",
            (submission_id, task_id, source, Json(selection), fingerprint),
        )
        cursor.execute(
            "SELECT * FROM processing.input_submissions WHERE submission_id=%s FOR UPDATE",
            (submission_id,),
        )
        receipt = dict(cursor.fetchone())
        if (
            str(receipt["task_id"]) != task_id
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id already belongs to a different selection or task"
            )
        if receipt["status"] == "cancelled":
            raise ValueError("Submission was cancelled")
        if receipt["status"] != "completed":
            cursor.execute(
                "UPDATE processing.input_submissions SET status='preparing',finished_at=NULL,error_message=NULL WHERE submission_id=%s",
                (submission_id,),
            )
        return receipt


def finish_submission(
    store: ProcessingStore, *, submission_id: str, task_id: str, count: int, total: int
) -> None:
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.input_submissions SET status='completed',input_count=%s,finished_at=now(),error_message=NULL WHERE submission_id=%s AND task_id=%s AND status='preparing'",
            (count, submission_id, task_id),
        )
        if cursor.rowcount != 1:
            raise ValueError("Submission is no longer preparing")
        cursor.execute(
            "UPDATE processing.tasks SET total=%s WHERE task_id=%s AND status='draft'",
            (total, task_id),
        )
        if cursor.rowcount != 1:
            raise ValueError("Queue was frozen during import")


def fail_submission(store: ProcessingStore, submission_id: str) -> None:
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.input_submissions SET status='failed',finished_at=now(),
            error_message='Import interrupted; retry the same submission_id and selection'
            WHERE submission_id=%s AND status='preparing'""",
            (submission_id,),
        )
