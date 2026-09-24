"""Scan fixed input tasks through the existing durable manifest protocol."""

import json
from collections import Counter
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.assets import monitor_webtech_scan
from dagster_v3.defs.webtech.execution import (
    start_execution,
    prepare_execution,
    read_plan_object,
    record_bucket,
    finish_execution,
    purge_completed_inputs,
)
from dagster_v3.defs.webtech.client import WebtechApiResource
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    FinalScanReference,
    SubmittedScanReference,
    WebtechCandidate,
)
from dagster_v3.defs.webtech.storage import (
    WebtechS3Destination,
    index_final_results,
    write_candidate_manifest,
    read_final_manifest,
)


class WebtechTaskConfig(dg.Config):
    task_id: str
    execution_id: str | None = None
    force_rescan: bool = False
    recent_days: int = Field(default=30, ge=1, le=3650)
    batch_size: int = Field(default=5000, ge=1, le=10000)

    @field_validator("execution_id")
    @classmethod
    def valid_execution(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("task_id")
    @classmethod
    def valid_task(cls, value: str) -> str:
        return str(UUID(value))


def complete_task(
    context: dg.AssetExecutionContext,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    task: dict,
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
    if failed:
        context.log.warning(
            "Completed with %s website errors. All outcomes are saved; queue inputs cleared.",
            failed,
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
        description="Start/freeze a draft, prepare freshness decisions, then scan and publish results. Same execution_id resumes. Fully processed tasks clear their inputs after publishing all outcomes; rescans use a new queue.",
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
            if task is None or task["processor"] != PROCESSOR_VERSION:
                raise ValueError(
                    "Prepare webtech_scan_input for this task_id before scanning"
                )
            draft_lifecycle = task["queue_scope"] is not None
            plan = None
            if draft_lifecycle:
                task = start_execution(
                    store=store,
                    clickhouse=clickhouse,
                    task_id=config.task_id,
                    execution_id=config.execution_id,
                    force_rescan=config.force_rescan,
                    recent_days=config.recent_days,
                    batch_size=config.batch_size,
                    run_id=context.run.run_id,
                )
                execution_id = task["config"]["execution"]["execution_id"]
                context.instance.add_run_tags(
                    context.run.run_id,
                    {
                        "processing/task_id": config.task_id,
                        "webtech/execution_id": execution_id,
                        "webtech/execution": json.dumps(
                            task["config"]["execution"], sort_keys=True
                        ),
                    },
                )
                if task["status"] == "completed":
                    completion = complete_task(context, store, clickhouse, task)
                    return dg.MaterializeResult(
                        metadata={
                            "task_id": config.task_id,
                            "execution_id": execution_id,
                            "already_completed": True,
                            **completion,
                        }
                    )
            else:
                if (
                    config.execution_id is not None
                    or config.force_rescan
                    or config.recent_days != 30
                    or config.batch_size != 5000
                ):
                    raise ValueError(
                        "Legacy tasks require their original processing settings"
                    )
                if (
                    task["status"] != "selected"
                    or task["source_info"].get("detector_version")
                    != WEBTECH_DETECTOR_VERSION
                ):
                    raise ValueError("Prepare the legacy Webtech input before scanning")
                execution_id = config.task_id
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
            if draft_lifecycle:
                plan = prepare_execution(
                    store=store,
                    clickhouse=clickhouse,
                    object_store=webtech_object_store,
                    task=task,
                )
            total = 0
            scans = []
            batch_numbers = (
                [row["bucket"] for row in plan["buckets"]]
                if plan is not None
                else range(128)
            )
            for bucket in batch_numbers:
                if plan is not None:
                    reference = next(
                        (row for row in plan["buckets"] if row["bucket"] == bucket),
                        None,
                    )
                    if reference is None:
                        continue
                    checkpoint = task["work_config"].get("buckets", {}).get(str(bucket))
                    if checkpoint is not None:
                        total += checkpoint["indexed"]
                        scans.append(checkpoint["scan_id"])
                        continue
                    document = read_plan_object(
                        webtech_object_store, reference["key"], reference["sha256"]
                    )
                    rows = [
                        (row["root_domain"], row["page_url"], row["input_id"])
                        for row in document["items"]
                        if row["decision"] == "scan"
                    ]
                else:
                    with clickhouse.get_connection() as client:
                        rows = client.execute(
                            f"SELECT root_domain,page_url,input_id FROM {INPUT_RELATION} "
                            "WHERE task_id=%(task)s AND bucket=%(bucket)s ORDER BY input_id",
                            {"task": config.task_id, "bucket": bucket},
                        )
                if not rows:
                    continue
                candidates = tuple(
                    WebtechCandidate(
                        root_domain=root,
                        page_url=page,
                        input_id=identity,
                        task_id=config.task_id,
                    )
                    for root, page, identity in rows
                )
                manifest = write_candidate_manifest(
                    object_store=webtech_object_store,
                    destination=destination,
                    crawl_id=f"webtech-{execution_id}",
                    partition_key=f"batch_{bucket:06d}"
                    if draft_lifecycle
                    else f"hash_{bucket:03d}",
                    dagster_run_id=execution_id,
                    candidates=candidates,
                    schema_version=3,
                )
                snapshot = webtech_api.submit(manifest)
                snapshot = monitor_webtech_scan(
                    context=context,
                    submission=SubmittedScanReference(
                        scan_id=snapshot.scan_id,
                        status=snapshot.status,
                        manifest=manifest,
                    ),
                    webtech_api=webtech_api,
                    webtech_object_store=webtech_object_store,
                    destination=destination,
                )
                reference = FinalScanReference(
                    scan_id=snapshot.scan_id,
                    crawl_id=snapshot.crawl_id,
                    partition_key=snapshot.partition_key,
                    detector_version=snapshot.detector_version,
                    uri=snapshot.final_manifest_uri,
                    total_count=snapshot.total_count,
                    outcome_counts=snapshot.outcome_counts,
                    technology_count=snapshot.technology_count,
                    elapsed_seconds=snapshot.elapsed_seconds,
                    domains_per_minute=snapshot.domains_per_minute,
                )
                if snapshot.total_count != len(rows) or sum(
                    snapshot.outcome_counts.values()
                ) != len(rows):
                    raise ValueError(
                        "Scanner results do not cover the submitted inputs"
                    )
                final = read_final_manifest(
                    object_store=webtech_object_store,
                    destination=destination,
                    reference=reference,
                )
                if draft_lifecycle and {row.input_id for row in final.results} != {
                    row[2] for row in rows
                }:
                    raise ValueError("Scanner returned different input identities")
                outcomes = dict(Counter(row.outcome for row in final.results))
                if (
                    outcomes != snapshot.outcome_counts
                    or outcomes != final.outcome_counts
                ):
                    raise ValueError(
                        "Scanner outcome counts do not match durable results"
                    )
                indexed = index_final_results(
                    clickhouse=clickhouse,
                    object_store=webtech_object_store,
                    destination=destination,
                    reference=reference,
                    dagster_run_id=context.run.run_id,
                )
                if indexed != len(rows):
                    raise ValueError("Not all submitted results were published")
                if draft_lifecycle:
                    record_bucket(
                        store,
                        config.task_id,
                        bucket,
                        {
                            "scan_id": snapshot.scan_id,
                            "indexed": indexed,
                            "succeeded": outcomes.get("success", 0),
                            "failed": indexed - outcomes.get("success", 0),
                        },
                    )
                total += indexed
                scans.append(snapshot.scan_id)
                context.log.info(
                    "Webtech task %s indexed %s/%s inputs",
                    config.task_id,
                    total,
                    task["total"],
                )
            completion = {}
            if draft_lifecycle:
                task = finish_execution(store, config.task_id)
                completion = complete_task(context, store, clickhouse, task)
            return dg.MaterializeResult(
                metadata={
                    "task_id": config.task_id,
                    "execution_id": execution_id,
                    "indexed_inputs": total,
                    "skipped_recent": plan["skipped"] if plan is not None else 0,
                    "scan_ids": scans,
                    **completion,
                }
            )

    return results
