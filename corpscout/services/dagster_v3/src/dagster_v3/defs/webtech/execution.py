"""Freeze draft membership; derive remaining work and completion from results."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from psycopg2.extras import Json
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingStore
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import WEBTECH_DETECTOR_VERSION

RESULT_RELATION = "corpscout.webtech_domain_scan_results"


def start_execution(
    *,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    task_id: str,
    execution_id: str | None,
    force_rescan: bool,
    recent_days: int,
    batch_size: int,
    run_id: str,
) -> dict:
    """Called under the same session lock used by imports and result processing."""
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != PROCESSOR_VERSION
        or task["queue_scope"] is None
    ):
        raise ValueError("Expected a Webtech draft queue")
    profile = {
        "force_rescan": force_rescan,
        "recent_days": recent_days,
        "batch_size": batch_size,
        "detector_version": WEBTECH_DETECTOR_VERSION,
    }
    saved = task["config"].get("execution")
    identity = execution_id or (saved["execution_id"] if saved else str(uuid4()))
    if saved is not None and identity == saved["execution_id"]:
        if saved["profile"] != profile:
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
    snapshot = ClickHouseInputQueue(
        clickhouse, INPUT_RELATION, selection_task_id=task_id
    ).inspect()
    if snapshot["total"] == 0:
        raise ValueError("Cannot start an empty queue")
    now = datetime.now(UTC)
    execution = {
        "execution_id": identity,
        "profile": profile,
        "dagster_run_id": run_id,
        "started_at": now.isoformat(),
        "freshness_cutoff": (now - timedelta(days=recent_days)).isoformat(),
    }
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='selected',frozen_at=coalesce(frozen_at,%s),
            completed_at=NULL,source_info=%s,total=%s,config=%s,work_config='{}',ready_at=NULL,
            admitted_count=0,succeeded_count=0,skipped_count=0,terminal_failed_count=0
            WHERE task_id=%s RETURNING *""",
            (
                now,
                Json(snapshot),
                snapshot["total"],
                Json({"execution": execution}),
                task_id,
            ),
        )
        return dict(cursor.fetchone())


def execution_crawl_id(execution: dict) -> str:
    return f"webtech-{execution['execution_id']}"


def _clickhouse_time(value: str) -> str:
    return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def _parameters(task: dict) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "crawl": execution_crawl_id(execution),
        "force": int(execution["profile"]["force_rescan"]),
        "detector": WEBTECH_DETECTOR_VERSION,
        "cutoff": _clickhouse_time(execution["freshness_cutoff"]),
        "started": _clickhouse_time(execution["started_at"]),
    }


# Frozen entries without a result in this execution, minus pages whose latest result
# inside the frozen freshness window is a success. The window ends at the execution's
# start, so results written by this execution never change the answer on resume.
_REMAINING = f"""
FROM {INPUT_RELATION}
WHERE task_id = %(task)s
  AND input_id NOT IN (
      SELECT input_id FROM {RESULT_RELATION}
      WHERE task_id = %(task)s AND crawl_id = %(crawl)s
        AND root_domain IN (SELECT root_domain FROM {INPUT_RELATION} WHERE task_id = %(task)s))
  AND (%(force)s = 1 OR (root_domain, website_origin, page_url) NOT IN (
      SELECT root_domain, website_origin, page_url
      FROM {RESULT_RELATION} FINAL
      WHERE detector_version = %(detector)s
        AND scanned_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
        AND scanned_at <= toDateTime64(%(started)s, 6, 'UTC')
        AND root_domain IN (SELECT root_domain FROM {INPUT_RELATION} WHERE task_id = %(task)s)
      GROUP BY root_domain, website_origin, page_url
      HAVING argMax(outcome, tuple(scanned_at, scan_id)) = 'success'))
"""


def remaining_inputs(
    client, task: dict, *, limit: int, input_ids: Sequence[str] | None = None
) -> list[tuple[str, str, str, str]]:
    if input_ids is not None and not input_ids:
        return []
    only = " AND input_id IN %(ids)s " if input_ids is not None else " "
    return [
        tuple(row)
        for row in client.execute(
            "SELECT input_id, root_domain, website_origin, page_url"
            + _REMAINING
            + only
            + "ORDER BY input_id LIMIT %(limit)s",
            {**_parameters(task), "limit": limit, "ids": tuple(input_ids or ())},
        )
    ]


def finish_execution(
    store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str
) -> dict:
    """Completion is derived from results; skipped pages are the fresh ones."""
    task = store.task(task_id)
    if task is None:
        raise ValueError("Unknown Webtech task")
    parameters = _parameters(task)
    with clickhouse.get_connection() as client:
        [(remaining,)] = client.execute("SELECT count()" + _REMAINING, parameters)
        if remaining:
            raise ValueError(
                f"Not every input has a published outcome ({remaining} remaining)"
            )
        [(succeeded, failed)] = client.execute(
            f"""SELECT countIf(outcome = 'success'), countIf(outcome != 'success') FROM (
                SELECT input_id, argMax(outcome, tuple(scanned_at, scan_id)) AS outcome
                FROM {RESULT_RELATION} FINAL
                WHERE task_id = %(task)s AND crawl_id = %(crawl)s
                GROUP BY input_id)""",
            parameters,
        )
    skipped = task["total"] - succeeded - failed
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET succeeded_count=%s, terminal_failed_count=%s,
            skipped_count=%s, admitted_count=%s, status='completed',
            work_config=work_config || '{"finished":true}'::jsonb,
            completed_at=coalesce(completed_at, now())
            WHERE task_id=%s RETURNING *""",
            (succeeded, failed, skipped, task["total"], task_id),
        )
        return dict(cursor.fetchone())


def purge_completed_inputs(
    store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str
) -> None:
    """Drop the completed task's partition. Results and task history remain."""
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != PROCESSOR_VERSION
        or task["queue_scope"] is None
        or task["status"] != "completed"
        or not task["work_config"].get("finished")
    ):
        raise ValueError("Only fully completed Webtech tasks can clear their inputs")
    if task["inputs_purged_at"] is not None:
        return
    with clickhouse.get_connection() as client:
        client.execute(
            f"ALTER TABLE {INPUT_RELATION} DROP PARTITION %(task)s", {"task": task_id}
        )
        [(left,)] = client.execute(
            f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
            {"task": task_id},
        )
        if left:
            raise RuntimeError("Completed Webtech input cleanup is not yet visible")
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=coalesce(inputs_purged_at, now()) WHERE task_id=%s",
            (task_id,),
        )
