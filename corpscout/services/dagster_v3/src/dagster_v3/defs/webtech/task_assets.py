"""Process a frozen webtech task in envelopes until nothing remains."""

import json
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.webtech.monitor import monitor_webtech_scan
from dagster_v3.defs.webtech.client import WebtechApiResource
from dagster_v3.defs.webtech.execution import (
    execution_crawl_id,
    finish_execution,
    purge_completed_inputs,
    remaining_inputs,
    start_execution,
)
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    WebtechCandidate,
)
from dagster_v3.defs.webtech.storage import (
    WebtechS3Destination,
    index_result_references,
)


class WebtechTaskConfig(dg.Config):
    task_id: str
    execution_id: str | None = None
    force_rescan: bool = False
    recent_days: int = Field(default=30, ge=1, le=3650)
    batch_size: int = Field(
        default=5000,
        ge=1,
        le=10000,
        description="Pages per scanner envelope. Transport only; may change between resumes.",
    )

    @field_validator("execution_id")
    @classmethod
    def valid_execution(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("task_id")
    @classmethod
    def valid_task(cls, value: str) -> str:
        return str(UUID(value))


def submit_envelope(api: WebtechApiResource, *, crawl_id: str, candidates):
    """Seam for tests; the scanner derives the scan ID from the envelope content."""
    return api.submit(crawl_id=crawl_id, candidates=candidates)


def complete_task(
    context, store: ProcessingStore, clickhouse: ClickhouseResource, task: dict
) -> dict:
    """Website errors are published outcomes; only pipeline errors fail the run."""
    failed = task["terminal_failed_count"]
    outcome = "completed_with_errors" if failed else "completed"
    purge_completed_inputs(store, clickhouse, str(task["task_id"]))
    context.instance.add_run_tags(
        context.run.run_id,
        {
            "webtech/outcome": outcome,
            "webtech/succeeded_pages": str(task["succeeded_count"]),
            "webtech/failed_pages": str(failed),
            "webtech/skipped_pages": str(task["skipped_count"]),
        },
    )
    return {
        "completion_status": outcome,
        "succeeded_pages": task["succeeded_count"],
        "failed_pages": failed,
        "skipped_recent": task["skipped_count"],
        "inputs_purged": True,
    }


def build_webtech_task_asset(destination: WebtechS3Destination):
    @dg.asset(
        name="webtech_scan_results",
        group_name="webtech",
        deps=["webtech_scan_input"],
        kinds={"clickhouse", "s3", "browser"},
        pool="webtech_remote_scanner",
        description="Freeze a draft, then send remaining pages to the scanner in envelopes and publish "
        "results as they arrive. The same execution_id resumes from what remains. Completed tasks "
        "drop their input partition; rescans use a new queue.",
    )
    def results(
        context: dg.AssetExecutionContext,
        config: WebtechTaskConfig,
        clickhouse: ClickhouseResource,
        processing: ProcessingResource,
        webtech_api: WebtechApiResource,
        webtech_object_store: ObjectStoreResource,
    ) -> dg.MaterializeResult:
        with processing.get_store() as store, store.selection_lock(config.task_id):
            task = store.task(config.task_id)
            if (
                task is None
                or task["processor"] != PROCESSOR_VERSION
                or task["queue_scope"] is None
            ):
                raise ValueError(
                    "Prepare webtech_scan_input for this task_id before scanning"
                )
            task = start_execution(
                store=store,
                clickhouse=clickhouse,
                task_id=config.task_id,
                execution_id=config.execution_id,
                force_rescan=config.force_rescan,
                recent_days=config.recent_days,
                run_id=context.run.run_id,
            )
            execution = task["config"]["execution"]
            crawl_id = execution_crawl_id(execution)
            context.instance.add_run_tags(
                context.run.run_id,
                {
                    "processing/task_id": config.task_id,
                    "webtech/execution_id": execution["execution_id"],
                    "webtech/execution": json.dumps(execution, sort_keys=True),
                },
            )
            if task["status"] == "completed":
                return dg.MaterializeResult(
                    metadata={
                        "task_id": config.task_id,
                        "execution_id": execution["execution_id"],
                        "already_completed": True,
                        **complete_task(context, store, clickhouse, task),
                    }
                )
            queue = ClickHouseInputQueue(
                clickhouse, INPUT_RELATION, selection_task_id=config.task_id
            )
            if queue.inspect() != {
                key: task["source_info"][key]
                for key in (
                    "relation",
                    "table_uuid",
                    "total",
                    "upper_id",
                    "selection_task_id",
                )
            }:
                raise ValueError("Input selection changed after preparation")

            published = 0

            def publish(references):
                nonlocal published
                published += index_result_references(
                    clickhouse=clickhouse,
                    object_store=webtech_object_store,
                    destination=destination,
                    crawl_id=crawl_id,
                    detector_version=WEBTECH_DETECTOR_VERSION,
                    references=references,
                    dagster_run_id=context.run.run_id,
                )

            buffer = ResultBuffer(publish, max_items=500, max_seconds=5.0)

            def publish_while_polling(flush) -> None:
                # A failed insert during polling keeps its rows buffered for the next
                # poll; only the end-of-envelope flush fails the run.
                try:
                    flush()
                except Exception as error:
                    context.log.warning(
                        "Webtech result publish failed while polling; %s results kept "
                        "for the next attempt: %s",
                        len(buffer),
                        error,
                    )

            def on_results(references) -> None:
                publish_while_polling(lambda: buffer.add(references))

            def on_poll() -> None:
                publish_while_polling(buffer.flush_if_due)

            envelopes = 0
            while True:
                with clickhouse.get_connection() as client:
                    rows = remaining_inputs(client, task, limit=config.batch_size)
                if not rows:
                    break
                envelopes += 1
                candidates = tuple(
                    WebtechCandidate(
                        root_domain=root,
                        page_url=page,
                        input_id=identity,
                        task_id=config.task_id,
                    )
                    for identity, root, _origin, page in rows
                )
                snapshot = submit_envelope(
                    webtech_api, crawl_id=crawl_id, candidates=candidates
                )
                snapshot = monitor_webtech_scan(
                    context,
                    scan_id=snapshot.scan_id,
                    crawl_id=crawl_id,
                    candidates=candidates,
                    webtech_api=webtech_api,
                    on_results=on_results,
                    on_poll=on_poll,
                )
                buffer.flush()
                # Reconcile: an event the monitor missed still left its page stored, and
                # the completed scan replays every result from event 0. Re-publishing a
                # page is idempotent.
                envelope_ids = [row[0] for row in rows]
                with clickhouse.get_connection() as client:
                    still = {
                        row[0]
                        for row in remaining_inputs(
                            client, task, limit=len(rows), input_ids=envelope_ids
                        )
                    }
                if still:
                    replay = webtech_api.poll(
                        snapshot.scan_id, after_event=0, wait_seconds=0
                    )
                    missing = [
                        item
                        for event in replay.events
                        for item in event.results
                        if item.input_id in still
                    ]
                    if missing:
                        buffer.add(missing)
                        buffer.flush()
                    with clickhouse.get_connection() as client:
                        unresolved = remaining_inputs(
                            client, task, limit=len(rows), input_ids=envelope_ids
                        )
                    if unresolved:
                        raise ValueError(
                            f"Scanner completed an envelope without results for {len(unresolved)} pages"
                        )
                context.log.info(
                    "Webtech task %s: envelope %s published, %s results so far",
                    config.task_id,
                    envelopes,
                    published,
                )
            task = finish_execution(store, clickhouse, config.task_id)
            return dg.MaterializeResult(
                metadata={
                    "task_id": config.task_id,
                    "execution_id": execution["execution_id"],
                    "envelopes": envelopes,
                    "published_results": published,
                    **complete_task(context, store, clickhouse, task),
                }
            )

    return results
