"""Periodic, durable Brave task progress, including workers already running."""

import json
from time import time

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.company_domains.assets import BraveSearchConfig
from dagster_v3.defs.company_domains.queue_execution import REMAINING, parameters
from dagster_v3.defs.company_domains.queue_tables import PROCESSOR
from dagster_v3.defs.company_domains.results import RESULT_TABLE


def read_progress(client, task: dict, run_id: str, skipped: int | None) -> dict:
    # Freshness is frozen at execution start. Count reused inputs once; subsequent
    # observations only aggregate the execution's saved results, not the full queue.
    if task["status"] == "completed":
        skipped = task["skipped_count"]
    remaining_sql = "NULL" if skipped is not None else "(SELECT count()" + REMAINING + ")"
    [(succeeded, failed, new_in_run, remaining)] = client.execute(
        f"""SELECT countIf(status='success'),countIf(status='error'),
            countIf(source_run_id=%(run)s),{remaining_sql}
        FROM (
            SELECT input_id,
                argMax(status,tuple(completed_at,result_id)) AS status,
                argMax(source_run_id,tuple(completed_at,result_id)) AS source_run_id
            FROM {RESULT_TABLE} FINAL
            WHERE task_id=%(task)s AND execution_id=%(execution)s
            GROUP BY input_id
        )""",
        {**parameters(task), "run": run_id},
        settings={"readonly": 2, "max_execution_time": 15},
    )
    processed = succeeded + failed
    if skipped is None:
        skipped = task["total"] - remaining - processed
    if skipped < 0 or processed + skipped > task["total"]:
        raise ValueError("Brave progress is inconsistent with the frozen input total")
    return {
        "total": task["total"], "processed": processed, "succeeded": succeeded,
        "failed": failed, "skipped": skipped, "new_in_run": new_in_run,
    }


@dg.sensor(minimum_interval_seconds=30, default_status=dg.DefaultSensorStatus.RUNNING)
def brave_progress_sensor(
    context: dg.SensorEvaluationContext,
    processing: ProcessingResource,
    processing_clickhouse: ClickhouseResource,
):
    previous = json.loads(context.cursor) if context.cursor else {}
    active = context.instance.get_run_records(filters=dg.RunsFilter(
        statuses=[dg.DagsterRunStatus.STARTED, dg.DagsterRunStatus.CANCELING],
    ))
    records = {
        record.dagster_run.run_id: record for record in active
        if record.dagster_run.tags.get("brave/service_batches") != "1"
        and "brave/execution_id" in record.dagster_run.tags
        and "processing/task_id" in record.dagster_run.tags
    }
    ended = list(previous.keys() - records.keys())
    if ended:
        records.update({
            record.dagster_run.run_id: record
            for record in context.instance.get_run_records(filters=dg.RunsFilter(run_ids=ended))
        })
    current = {}
    for run_id, record in records.items():
        run = record.dagster_run
        if run.tags.get("brave/service_batches") == "1":
            continue
        task_id = run.tags["processing/task_id"]
        with processing.get_store() as store:
            task = store.task(task_id)
        if (
            task is None or task["processor"] != PROCESSOR or task["queue_scope"] is None
            or task["config"].get("execution", {}).get("execution_id") != run.tags["brave/execution_id"]
        ):
            continue
        last = previous.get(run_id)
        with processing_clickhouse.get_connection() as client:
            counts = read_progress(client, task, run_id, last["skipped"] if last else None)
        now = time()
        settings = BraveSearchConfig(**run.run_config.get("ops", {}).get(
            "company_brave_search_results", {}).get("config", {}))
        elapsed = now - last["at"] if last else 0
        newly_saved = counts["new_in_run"] - last["new_in_run"] if last else 0
        if last and not run.is_finished and (
            newly_saved < settings.progress_log_every
            and elapsed < settings.progress_log_interval_seconds
        ):
            current[run_id] = last
            continue
        recent_speed = f"{60 * newly_saved / elapsed:.2f}" if last and elapsed > 0 else "n/a"
        run_seconds = (record.end_time or now) - (record.start_time or record.create_timestamp.timestamp())
        average_speed = f"{60 * counts['new_in_run'] / run_seconds:.2f}" if run_seconds > 0 else "n/a"
        done = counts["processed"] + counts["skipped"]
        percent = 100 * done / counts["total"] if counts["total"] else 100
        message = (
            f"Brave progress | completed={done:,}/{counts['total']:,} ({percent:.2f}%)"
            f" | processed={counts['processed']:,} successful={counts['succeeded']:,}"
            f" failed={counts['failed']:,} reused={counts['skipped']:,}"
            f" | remaining={counts['total'] - done:,}"
            f" | speed={recent_speed} entries/min (last {elapsed:.0f}s)"
            f" run_average={average_speed} entries/min new_in_run={counts['new_in_run']:,}"
            f" | state={run.status.value} task={task_id}"
        )
        context.instance.report_engine_event(
            message, dagster_run=run, step_key="company_brave_search_results",
        )
        if not run.is_finished:
            current[run_id] = {"at": now, **counts}
    context.update_cursor(json.dumps(current))
    return dg.SkipReason(f"Observed progress for {len(records)} Brave runs; no work launched")
