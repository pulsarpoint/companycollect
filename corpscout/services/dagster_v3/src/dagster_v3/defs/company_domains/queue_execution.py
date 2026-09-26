"""Frozen Brave drafts; saved outcomes are the checkpoint for every company."""

import json
from contextlib import closing
from datetime import datetime
from uuid import UUID, uuid5

import dagster as dg

from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import render_query
from dagster_v3.defs.company_domains.browser import CompanySearchInput
from dagster_v3.defs.company_domains.queue_tables import INPUT_RELATION, PROCESSOR
from dagster_v3.defs.company_domains.result_writer import BraveResultWriter
from dagster_v3.defs.company_domains.results import (
    RESULT_TABLE,
    repair_current_answers,
    result_record,
)


REMAINING = f"""
FROM {INPUT_RELATION}
WHERE task_id = %(task)s
 AND input_id NOT IN (
   SELECT input_id FROM {RESULT_TABLE} WHERE task_id = %(task)s AND execution_id = %(execution)s)
 AND (%(force)s = 1 OR (country_code, company_id) NOT IN (
   SELECT country_code, company_id FROM {RESULT_TABLE} FINAL
   WHERE query_type = %(query_type)s
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
        profile = execution["profile"]
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
            pending = {}

            def companies():
                after = ""
                while True:
                    with clickhouse.get_connection() as client:
                        rows = remaining_inputs(
                            client, task, limit=config.input_batch_size, after=after
                        )
                    if not rows:
                        return
                    for values in rows:
                        request_id = str(
                            uuid5(UUID(execution["execution_id"]), values["input_id"])
                        )
                        pending[request_id] = values
                        yield CompanySearchInput(
                            values["input_id"],
                            values["company_name"],
                            render_query(profile["query_template"], values),
                            request_id,
                            config.answer_timeout_seconds * 1000,
                        )
                    after = rows[-1]["input_id"]

            with BraveResultWriter(
                processing_clickhouse, max_items=200, max_seconds=5.0
            ) as writer:

                def save(result):
                    if result.error_stage == "browser_service":
                        raise RuntimeError(
                            f"Brave browser service failed ({result.error_type}); resume this task"
                        )
                    record = result_record(
                        result,
                        pending[result.company.request_id],
                        task_id=task_id,
                        execution_id=execution["execution_id"],
                        query_type=profile["query_type"],
                        source_run_id=context.run.run_id,
                        processor_version=PROCESSOR,
                    )
                    writer.save(record)
                    del pending[result.company.request_id]

                with closing(
                    browser.iter_answers(
                        companies(),
                        requests_per_route=config.requests_per_route,
                        on_result=save,
                        llm=profile["llm"],
                    )
                ) as answers:
                    count = 0
                    for result in answers:
                        count += 1
                        if count == 1 or count % config.progress_log_every == 0:
                            context.log.info(
                                "Brave task %s: %s new results published",
                                task_id,
                                count,
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
