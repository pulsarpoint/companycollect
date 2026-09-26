"""Frozen Brave drafts; saved outcomes are the checkpoint for every company."""

import json
from datetime import datetime

import dagster as dg

from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.company_domains.batches import process_batches
from dagster_v3.defs.company_domains.queue_tables import INPUT_RELATION, PROCESSOR
from dagster_v3.defs.company_domains.results import (
    RESULT_TABLE,
    repair_current_answers,
)

REMAINING = f"""
FROM {INPUT_RELATION}
WHERE task_id = %(task)s
 AND input_id NOT IN (
   SELECT input_id FROM {RESULT_TABLE} WHERE task_id = %(task)s AND execution_id = %(execution)s)
 AND (%(force)s = 1 OR (country_code, company_id) NOT IN (
   SELECT country_code, company_id FROM {RESULT_TABLE} FINAL
   WHERE query_type = %(query_type)s
     AND (%(search_id)s = '' OR (search_id = %(search_id)s AND search_revision = %(search_revision)s))
     AND completed_at >= toDateTime64(%(cutoff)s,6,'UTC')
     AND completed_at <= toDateTime64(%(started)s,6,'UTC')
     AND (country_code,company_id) IN (
       SELECT country_code,company_id FROM {INPUT_RELATION} WHERE task_id=%(task)s)
   GROUP BY country_code,company_id
   HAVING argMax(status,tuple(completed_at,result_id)) = 'success'))
"""


def parameters(task: dict) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "execution": execution["execution_id"],
        "force": int(execution["profile"]["force_rescan"]),
        "query_type": execution["profile"]["query_type"],
        "search_id": execution["profile"].get("search_id", ""),
        "search_revision": execution["profile"].get("search_revision", 0),
        "cutoff": datetime.fromisoformat(execution["freshness_cutoff"]).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        "started": datetime.fromisoformat(execution["started_at"]).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
    }


def remaining_inputs(client, task: dict, *, limit: int, after: str = "") -> list[dict]:
    rows, columns = client.execute(
        "SELECT input_id,country_code,company_id,company_name"
        + REMAINING
        + " AND input_id > %(after)s ORDER BY input_id LIMIT %(limit)s",
        {**parameters(task), "after": after, "limit": limit},
        with_column_types=True,
    )
    return [
        dict(zip((column[0] for column in columns), row, strict=True)) for row in rows
    ]


def start_execution(
    store, clickhouse, *, task_id: str, config, supplied: dict, run_id: str
):
    task = store.task(task_id)
    saved = task["config"].get("execution")
    if any(key in supplied for key in ("force", "rescan_old", "input_relation")):
        raise ValueError(
            "Drafts use force_rescan/recent_days and their saved input table"
        )
    if config.mode != "process":
        raise ValueError(
            "Draft queues use process mode; saved outcomes are repaired on resume"
        )
    fields = ("query_type", "query_template", "force_rescan", "recent_days")
    profile = {
        name: (
            saved["profile"][name]
            if saved and name not in supplied
            else getattr(config, name)
        )
        for name in fields
    }
    search_fields = ("search_id", "search_name", "search_revision")
    if any(getattr(config, name) is not None for name in search_fields) or (
        saved and "search_id" in saved["profile"]
    ):
        for name in search_fields:
            profile[name] = (
                saved["profile"].get(name)
                if saved and name not in supplied
                else getattr(config, name)
            )
        if any(profile[name] is None for name in search_fields):
            raise ValueError("Saved Brave searches require an ID, name and revision")
    llm = config.llm.model_dump(exclude_none=True) if config.llm is not None else None
    if saved:
        original = saved["profile"]["llm"]
        if llm is not None and {
            k: v for k, v in llm.items() if k != "api_key_encrypted"
        } != {k: v for k, v in original.items() if k != "api_key_encrypted"}:
            raise ValueError("Resume must keep the original LLM profile and revision")
        llm = original
    if llm is None:
        raise ValueError(
            "Choose a verified browser-assistant LLM before starting a Brave draft"
        )
    profile["llm"] = llm

    def snapshot():
        info = ClickHouseInputQueue(
            clickhouse, INPUT_RELATION, selection_task_id=task_id
        ).inspect()
        return info, info["total"]

    task = queue_execution.start_execution(
        store,
        task_id=task_id,
        processor=PROCESSOR,
        profile=profile,
        execution_id=config.execution_id,
        freshness_days=profile["recent_days"],
        run_id=run_id,
        snapshot=snapshot,
        default_execution_id=run_id,
        label="Brave",
    )
    return task


def finish_execution(store, clickhouse, task: dict):
    with clickhouse.get_connection() as client:
        repair_current_answers(
            client, execution_id=task["config"]["execution"]["execution_id"]
        )
        [(remaining,)] = client.execute("SELECT count()" + REMAINING, parameters(task))
        [(succeeded, failed)] = client.execute(
            f"SELECT countIf(status='success'),countIf(status='error') FROM {RESULT_TABLE} FINAL "
            "WHERE task_id=%(task)s AND execution_id=%(execution)s",
            parameters(task),
        )
    return queue_execution.record_completion(
        store,
        task_id=str(task["task_id"]),
        remaining=remaining,
        succeeded=succeeded,
        failed=failed,
    )


def run_draft(
    context,
    config,
    clickhouse,
    processing_clickhouse,
    browser,
    processing,
    task_id: str,
):
    supplied = (
        context.run.run_config.get("ops", {})
        .get("company_brave_search_results", {})
        .get("config", {})
    )
    with processing.get_store() as store, store.selection_lock(task_id):
        task = start_execution(
            store,
            clickhouse,
            task_id=task_id,
            config=config,
            supplied=supplied,
            run_id=context.run.run_id,
        )
        execution = task["config"]["execution"]
        context.instance.add_run_tags(
            context.run.run_id,
            {
                "processing/task_id": task_id,
                "brave/execution_id": execution["execution_id"],
                "brave/execution": json.dumps(execution),
            },
        )
        if task["status"] != "completed":
            queue = ClickHouseInputQueue(
                clickhouse, INPUT_RELATION, selection_task_id=task_id
            )
            if queue.inspect() != task["source_info"]:
                raise ValueError(
                    "Frozen Brave inputs changed; restore the saved selection before resuming"
                )

            def read_counts():
                with processing_clickhouse.get_connection() as client:
                    [(remaining,)] = client.execute(
                        "SELECT count()" + REMAINING, parameters(task)
                    )
                    [(succeeded, failed)] = client.execute(
                        f"SELECT countIf(status='success'),countIf(status='error') FROM {RESULT_TABLE} FINAL "
                        "WHERE task_id=%(task)s AND execution_id=%(execution)s",
                        parameters(task),
                    )
                return {
                    "processed": succeeded + failed,
                    "succeeded": succeeded,
                    "failed": failed,
                    "skipped": task["total"] - remaining - succeeded - failed,
                }

            def load_inputs():
                with processing_clickhouse.get_connection() as client:
                    return remaining_inputs(client, task, limit=config.input_batch_size)

            def confirm_results(result_ids):
                with processing_clickhouse.get_connection() as client:
                    [(count,)] = client.execute(
                        f"SELECT uniqExact(result_id) FROM {RESULT_TABLE} FINAL "
                        "WHERE task_id=%(task)s AND execution_id=%(execution)s AND result_id IN %(ids)s",
                        {**parameters(task), "ids": tuple(result_ids)},
                    )
                if count != len(result_ids):
                    raise RuntimeError(
                        "Brave service results are not visible in Dagster's ClickHouse database"
                    )

            process_batches(
                context,
                config,
                browser,
                task,
                load_inputs,
                read_counts,
                confirm_results,
            )
            task = finish_execution(store, processing_clickhouse, task)
        metadata = queue_execution.complete_task(
            context,
            store,
            clickhouse,
            task,
            processor=PROCESSOR,
            relation=INPUT_RELATION,
            tag_prefix="brave",
            label="Brave",
        )
        return dg.MaterializeResult(
            metadata={
                "task_id": task_id,
                "execution_id": execution["execution_id"],
                "total": task["total"],
                **metadata,
            }
        )
