"""Queue-contract lifecycle shared by processors: freeze, finish from counts, purge.

A processor keeps its own SQL (which entries remain, which are fresh) and its own
transport (scanner envelopes, windows of crawler requests). This module owns what
every processor does identically against PostgreSQL ``processing.tasks``: freezing an
open draft into an execution, recording completion counts derived from results, and
dropping the task's ClickHouse partition once every entry has an outcome.
"""

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from dagster_clickhouse import ClickhouseResource
from psycopg2.extras import Json

from dagster_v3.defs.common.processing import ProcessingStore


def start_execution(
    store: ProcessingStore,
    *,
    task_id: str,
    processor: str,
    profile: dict,
    execution_id: str | None,
    freshness_days: int,
    run_id: str,
    snapshot: Callable[[], tuple[dict, int]],
    transport_keys: Sequence[str] = (),
    default_execution_id: str | None = None,
    label: str = "queue",
) -> dict:
    """Freeze the draft, or return its saved execution for a resume.

    Called under the task's ``selection_lock``. ``snapshot`` reads the frozen entry
    set and returns ``(source_info, total)``. ``transport_keys`` name profile entries
    that may change between resumes (older executions froze them; they are ignored
    when comparing). ``default_execution_id`` identifies a brand-new execution when
    the caller passes no ``execution_id``; a fresh UUID is used otherwise.
    """
    task = store.task(task_id)
    if task is None or task["processor"] != processor or task["queue_scope"] is None:
        raise ValueError(f"Expected a {label} draft queue")
    saved = task["config"].get("execution")
    identity = execution_id or (
        saved["execution_id"] if saved else default_execution_id or str(uuid4())
    )
    if saved is not None and identity == saved["execution_id"]:
        frozen = {k: v for k, v in saved["profile"].items() if k not in transport_keys}
        wanted = {k: v for k, v in profile.items() if k not in transport_keys}
        if frozen != wanted:
            raise ValueError(
                "Execution settings are frozen; resume with the same profile"
            )
        if task["status"] not in ("selected", "ready", "completed"):
            raise ValueError("Execution is not resumable")
        return task
    if task["inputs_purged_at"] is not None:
        raise ValueError("Inputs were purged; create a new draft")
    if task["status"] != "draft" and not task["work_config"].get("finished"):
        raise ValueError(
            "Finish or resume the existing execution before starting another"
        )
    if task["status"] not in ("draft", "ready", "completed"):
        raise ValueError("Queue cannot be started in this state")
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) AS pending FROM processing.input_submissions WHERE task_id=%s AND status NOT IN ('completed','cancelled')",
            (task_id,),
        )
        if cursor.fetchone()["pending"]:
            raise ValueError("Finish or retry all outstanding imports before Start")
    source_info, total = snapshot()
    if total == 0:
        raise ValueError("Cannot start an empty queue")
    now = datetime.now(UTC)
    execution = {
        "execution_id": identity,
        "profile": profile,
        "dagster_run_id": run_id,
        "started_at": now.isoformat(),
        "freshness_cutoff": (now - timedelta(days=freshness_days)).isoformat(),
    }
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='selected',frozen_at=coalesce(frozen_at,%s),
            completed_at=NULL,source_info=%s,total=%s,config=%s,work_config='{}',ready_at=NULL,
            admitted_count=0,succeeded_count=0,skipped_count=0,terminal_failed_count=0
            WHERE task_id=%s RETURNING *""",
            (now, Json(source_info), total, Json({"execution": execution}), task_id),
        )
        return dict(cursor.fetchone())


def record_completion(
    store: ProcessingStore,
    *,
    task_id: str,
    remaining: int,
    succeeded: int,
    failed: int,
) -> dict:
    """Completion is derived from results; skipped entries are the rest of the total."""
    if remaining:
        raise ValueError(
            f"Not every input has a published outcome ({remaining} remaining)"
        )
    task = store.task(task_id)
    if task is None:
        raise ValueError("Unknown queue task")
    skipped = task["total"] - succeeded - failed
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET succeeded_count=%s, terminal_failed_count=%s,
            skipped_count=%s, admitted_count=%s, status='completed',
            work_config=work_config || '{"finished":true}'::jsonb,
            -- frozen_at comes from the Dagster host clock; never finish before it.
            completed_at=coalesce(completed_at, greatest(frozen_at, now()))
            WHERE task_id=%s RETURNING *""",
            (succeeded, failed, skipped, task["total"], task_id),
        )
        return dict(cursor.fetchone())


def purge_completed_inputs(
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    *,
    task_id: str,
    processor: str,
    relation: str,
    label: str = "queue",
) -> None:
    """Drop the completed task's partition. Results and task history remain."""
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != processor
        or task["queue_scope"] is None
        or task["status"] != "completed"
        or not task["work_config"].get("finished")
    ):
        raise ValueError(f"Only fully completed {label} tasks can clear their inputs")
    if task["inputs_purged_at"] is not None:
        return
    with clickhouse.get_connection() as client:
        client.execute(
            f"ALTER TABLE {relation} DROP PARTITION %(task)s", {"task": task_id}
        )
        [(left,)] = client.execute(
            f"SELECT count() FROM {relation} WHERE task_id=%(task)s", {"task": task_id}
        )
        if left:
            raise RuntimeError(f"Completed {label} input cleanup is not yet visible")
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=coalesce(inputs_purged_at, greatest(completed_at, now())) WHERE task_id=%s",
            (task_id,),
        )


def complete_task(
    context,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    task: dict,
    *,
    processor: str,
    relation: str,
    tag_prefix: str,
    label: str = "queue",
) -> dict:
    """Website errors are published outcomes; only pipeline errors fail the run."""
    failed = task["terminal_failed_count"]
    outcome = "completed_with_errors" if failed else "completed"
    purge_completed_inputs(
        store,
        clickhouse,
        task_id=str(task["task_id"]),
        processor=processor,
        relation=relation,
        label=label,
    )
    context.instance.add_run_tags(
        context.run.run_id,
        {
            f"{tag_prefix}/outcome": outcome,
            f"{tag_prefix}/succeeded_pages": str(task["succeeded_count"]),
            f"{tag_prefix}/failed_pages": str(failed),
            f"{tag_prefix}/skipped_pages": str(task["skipped_count"]),
        },
    )
    return {
        "completion_status": outcome,
        "succeeded_pages": task["succeeded_count"],
        "failed_pages": failed,
        "skipped_recent": task["skipped_count"],
        "inputs_purged": True,
    }
