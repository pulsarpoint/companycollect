"""Freeze draft membership; derive remaining work and completion from results."""

from collections.abc import Sequence
from datetime import datetime

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common import queue_execution
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
    run_id: str,
) -> dict:
    """Called under the same session lock used by imports and result processing."""
    profile = {
        "force_rescan": force_rescan,
        "recent_days": recent_days,
        "detector_version": WEBTECH_DETECTOR_VERSION,
    }

    def snapshot() -> tuple[dict, int]:
        inspected = ClickHouseInputQueue(
            clickhouse, INPUT_RELATION, selection_task_id=task_id
        ).inspect()
        return inspected, inspected["total"]

    # Envelope size is transport only. Older executions froze it; ignore it.
    return queue_execution.start_execution(
        store,
        task_id=task_id,
        processor=PROCESSOR_VERSION,
        profile=profile,
        execution_id=execution_id,
        freshness_days=recent_days,
        run_id=run_id,
        snapshot=snapshot,
        transport_keys=("batch_size",),
        label="Webtech",
    )


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
        [(succeeded, failed)] = client.execute(
            f"""SELECT countIf(outcome = 'success'), countIf(outcome != 'success') FROM (
                SELECT input_id, argMax(outcome, tuple(scanned_at, scan_id)) AS outcome
                FROM {RESULT_RELATION} FINAL
                WHERE task_id = %(task)s AND crawl_id = %(crawl)s
                  AND root_domain IN (SELECT root_domain FROM {INPUT_RELATION} WHERE task_id = %(task)s)
                GROUP BY input_id)""",
            parameters,
        )
    return queue_execution.record_completion(
        store, task_id=task_id, remaining=remaining, succeeded=succeeded, failed=failed
    )


def purge_completed_inputs(
    store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str
) -> None:
    """Drop the completed task's partition. Results and task history remain."""
    queue_execution.purge_completed_inputs(
        store,
        clickhouse,
        task_id=task_id,
        processor=PROCESSOR_VERSION,
        relation=INPUT_RELATION,
        label="Webtech",
    )
