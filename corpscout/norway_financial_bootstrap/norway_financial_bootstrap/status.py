import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import boto3
from dotenv import load_dotenv
from temporalio.api.enums.v1 import EventType, TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client

from norway_financial_bootstrap.cli import FIXED_WORKFLOW_ID
from norway_financial_bootstrap.paths import DEFAULT_BUCKET, RAW_REPORT_PREFIX
from norway_financial_bootstrap.workflows import (
    DEFAULT_SLOT_COUNT,
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    slot_workflow_id,
)

REQUIRED_S3_ENV_VARS = (
    "CORPSCOUT_S3_ENDPOINT",
    "CORPSCOUT_S3_ACCESS_KEY",
    "CORPSCOUT_S3_SECRET_KEY",
)
FAILED_MARKER_SUFFIX = "/status/failed.json"
DONE_MARKER_SUFFIX = "/status/done.json"


@dataclass(frozen=True)
class S3Summary:
    bucket: str
    prefix: str
    total_count: int
    raw_report_count: int
    done_marker_count: int
    failed_marker_count: int
    other_count: int
    latest_key: str | None
    latest_modified: str | None
    failed_marker_keys: list[str]


@dataclass(frozen=True)
class SlotSummary:
    workflow_id: str
    run_id: str | None
    status: str
    workflow_type: str | None
    history_length: int | None
    pending_activities: list[str]
    latest_event_type: str | None
    current_failure_count: int


@dataclass(frozen=True)
class VisibilitySummary:
    counts_by_status: dict[str, int]
    counts_by_type: dict[str, int]


@dataclass(frozen=True)
class TaskQueuePollers:
    workflow_poller_count: int
    activity_poller_count: int


@dataclass(frozen=True)
class BootstrapStatus:
    temporal_address: str
    task_queue: str
    workflow_id_prefix: str
    slots: list[SlotSummary]
    visibility: VisibilitySummary
    task_queue_pollers: TaskQueuePollers
    s3: S3Summary
    s3_after_compare: S3Summary | None
    verdict: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="norway-financial-bootstrap-status",
        description="Show Temporal and S3 status for the Norway BRREG financial bootstrap.",
    )
    parser.add_argument(
        "--temporal-address",
        default=None,
        help=(
            "Temporal frontend address. Defaults to TEMPORAL_ADDRESS or "
            f"{DEFAULT_TEMPORAL_ADDRESS}."
        ),
    )
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="S3-compatible endpoint URL. Defaults to CORPSCOUT_S3_ENDPOINT.",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"S3 bucket to inspect. Defaults to {DEFAULT_BUCKET}.",
    )
    parser.add_argument(
        "--prefix",
        default=RAW_REPORT_PREFIX,
        help=f"S3 raw report prefix to inspect. Defaults to {RAW_REPORT_PREFIX}.",
    )
    parser.add_argument(
        "--compare-after-seconds",
        type=int,
        default=0,
        help="Read S3 again after this many seconds and report whether counts moved.",
    )
    parser.add_argument(
        "--failed-samples",
        type=int,
        default=5,
        help="Maximum number of failed marker payloads to print.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of the human summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv(".env", override=False)
    if args.s3_endpoint is not None:
        os.environ["CORPSCOUT_S3_ENDPOINT"] = args.s3_endpoint

    try:
        status = asyncio.run(
            collect_status(
                temporal_address=args.temporal_address
                or os.environ.get("TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS),
                bucket=args.bucket,
                prefix=args.prefix,
                compare_after_seconds=args.compare_after_seconds,
            )
        )
        if args.json:
            sys.stdout.write(json.dumps(asdict(status), indent=2, sort_keys=True))
            sys.stdout.write("\n")
        else:
            print_human_status(status, failed_samples=args.failed_samples)
    except Exception as exc:
        sys.stderr.write(f"failed to read Norway financial bootstrap status: {exc}\n")
        return 1

    if status.verdict in {"NO_RUNNING_SLOT_WORKFLOWS", "NO_WORKER_POLLERS"}:
        return 2
    return 0


async def collect_status(
    *,
    temporal_address: str,
    bucket: str,
    prefix: str,
    compare_after_seconds: int,
) -> BootstrapStatus:
    client = await Client.connect(temporal_address)
    s3_client = _s3_client_from_env()

    slots, visibility, task_queue_pollers, s3_summary = await asyncio.gather(
        _slot_summaries(client),
        _visibility_summary(client),
        _task_queue_pollers(client),
        asyncio.to_thread(_s3_summary, s3_client, bucket=bucket, prefix=prefix),
    )
    s3_after_compare = None
    if compare_after_seconds > 0:
        time.sleep(compare_after_seconds)
        s3_after_compare = await asyncio.to_thread(
            _s3_summary,
            s3_client,
            bucket=bucket,
            prefix=prefix,
        )

    return BootstrapStatus(
        temporal_address=temporal_address,
        task_queue=DEFAULT_TASK_QUEUE,
        workflow_id_prefix=FIXED_WORKFLOW_ID,
        slots=slots,
        visibility=visibility,
        task_queue_pollers=task_queue_pollers,
        s3=s3_summary,
        s3_after_compare=s3_after_compare,
        verdict=classify_status(
            slots=slots,
            pollers=task_queue_pollers,
            s3_before=s3_summary,
            s3_after=s3_after_compare,
        ),
    )


def classify_status(
    *,
    slots: list[SlotSummary],
    pollers: TaskQueuePollers,
    s3_before: S3Summary,
    s3_after: S3Summary | None,
) -> str:
    running_slots = [slot for slot in slots if slot.status == "RUNNING"]
    if not running_slots:
        return "NO_RUNNING_SLOT_WORKFLOWS"
    if pollers.workflow_poller_count == 0 or pollers.activity_poller_count == 0:
        return "NO_WORKER_POLLERS"
    if any(slot.current_failure_count > 0 for slot in running_slots):
        return "CURRENT_SLOT_FAILURES"
    if s3_after is not None and _s3_progress_count(s3_after) == _s3_progress_count(
        s3_before
    ):
        if all(
            "fetch_and_store_candidate" in slot.pending_activities
            for slot in running_slots
        ):
            return "RUNNING_FETCH_BACKOFF_OR_STALLED"
        return "RUNNING_NO_S3_PROGRESS"
    return "RUNNING"


def print_human_status(status: BootstrapStatus, *, failed_samples: int) -> None:
    print(f"Temporal: {status.temporal_address}")
    print(f"Task queue: {status.task_queue}")
    print(
        "Pollers: "
        f"workflow={status.task_queue_pollers.workflow_poller_count} "
        f"activity={status.task_queue_pollers.activity_poller_count}"
    )
    print("Slots:")
    for slot in status.slots:
        pending = ", ".join(slot.pending_activities) or "-"
        print(
            f"  {slot.workflow_id}: status={slot.status} run_id={slot.run_id or '-'} "
            f"history={slot.history_length if slot.history_length is not None else '-'} "
            f"pending={pending} failures={slot.current_failure_count} "
            f"latest={slot.latest_event_type or '-'}"
        )

    print("Visibility:")
    print(f"  by_status={status.visibility.counts_by_status}")
    print(f"  by_type={status.visibility.counts_by_type}")

    _print_s3_summary("S3", status.s3)
    if status.s3_after_compare is not None:
        _print_s3_summary("S3 after compare", status.s3_after_compare)
        print(
            "S3 progress delta: "
            f"{_s3_progress_count(status.s3_after_compare) - _s3_progress_count(status.s3)}"
        )

    if failed_samples > 0 and status.s3.failed_marker_keys:
        print("Failed marker samples:")
        s3_client = _s3_client_from_env()
        for key in status.s3.failed_marker_keys[:failed_samples]:
            print(f"  {key}")
            try:
                payload = _read_json_object(
                    s3_client,
                    bucket=status.s3.bucket,
                    key=key,
                )
            except Exception as exc:
                print(f"    failed to read marker: {exc}")
                continue
            print(
                "    "
                f"org={payload.get('org_number', '-')} "
                f"status={payload.get('fetch_status', '-')} "
                f"error_type={payload.get('error_type', '-')} "
                f"message={payload.get('error_message', '-')}"
            )

    print(f"Verdict: {status.verdict}")


async def _slot_summaries(client: Client) -> list[SlotSummary]:
    summaries: list[SlotSummary] = []
    for slot_id in range(DEFAULT_SLOT_COUNT):
        workflow_id = slot_workflow_id(FIXED_WORKFLOW_ID, slot_id)
        summaries.append(await _slot_summary(client, workflow_id))
    return summaries


async def _slot_summary(client: Client, workflow_id: str) -> SlotSummary:
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe()
    except Exception:
        return SlotSummary(
            workflow_id=workflow_id,
            run_id=None,
            status="NOT_FOUND",
            workflow_type=None,
            history_length=None,
            pending_activities=[],
            latest_event_type=None,
            current_failure_count=0,
        )

    pending_activities, latest_event_type, current_failure_count = (
        await _workflow_history_state(handle)
    )
    return SlotSummary(
        workflow_id=workflow_id,
        run_id=description.run_id,
        status=description.status.name if description.status is not None else "UNKNOWN",
        workflow_type=description.workflow_type,
        history_length=description.history_length,
        pending_activities=pending_activities,
        latest_event_type=latest_event_type,
        current_failure_count=current_failure_count,
    )


async def _workflow_history_state(handle: Any) -> tuple[list[str], str | None, int]:
    scheduled: dict[int, str] = {}
    closed_scheduled_event_ids: set[int] = set()
    latest_event_type: str | None = None
    current_failure_count = 0

    async for event in handle.fetch_history_events():
        latest_event_type = EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            attrs = event.activity_task_scheduled_event_attributes
            scheduled[event.event_id] = attrs.activity_type.name
        elif event.event_type in {
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
        }:
            closed_scheduled_event_ids.add(_activity_closed_scheduled_event_id(event))
        elif event.event_type in {
            EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
        }:
            current_failure_count += 1

    pending = [
        name
        for event_id, name in scheduled.items()
        if event_id not in closed_scheduled_event_ids
    ]
    return pending, latest_event_type, current_failure_count


def _activity_closed_scheduled_event_id(event: Any) -> int:
    if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        return event.activity_task_completed_event_attributes.scheduled_event_id
    if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
        return event.activity_task_failed_event_attributes.scheduled_event_id
    if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        return event.activity_task_timed_out_event_attributes.scheduled_event_id
    if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
        return event.activity_task_canceled_event_attributes.scheduled_event_id
    return 0


async def _visibility_summary(client: Client) -> VisibilitySummary:
    counts_by_status: dict[str, int] = {}
    counts_by_type: dict[str, int] = {}
    query = f'WorkflowId STARTS_WITH "{FIXED_WORKFLOW_ID}"'
    async for workflow in client.list_workflows(query=query):
        status = workflow.status.name if workflow.status is not None else "UNKNOWN"
        counts_by_status[status] = counts_by_status.get(status, 0) + 1
        counts_by_type[workflow.workflow_type] = (
            counts_by_type.get(workflow.workflow_type, 0) + 1
        )
    return VisibilitySummary(
        counts_by_status=counts_by_status,
        counts_by_type=counts_by_type,
    )


async def _task_queue_pollers(client: Client) -> TaskQueuePollers:
    workflow_response, activity_response = await asyncio.gather(
        _describe_task_queue(client, TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW),
        _describe_task_queue(client, TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY),
    )
    return TaskQueuePollers(
        workflow_poller_count=len(workflow_response.pollers),
        activity_poller_count=len(activity_response.pollers),
    )


async def _describe_task_queue(client: Client, task_queue_type: int) -> Any:
    return await client.workflow_service.describe_task_queue(
        DescribeTaskQueueRequest(
            namespace=client.namespace,
            task_queue=TaskQueue(name=DEFAULT_TASK_QUEUE),
            task_queue_type=task_queue_type,
        )
    )


def _s3_client_from_env() -> Any:
    missing = [
        name
        for name in REQUIRED_S3_ENV_VARS
        if os.environ.get(name) is None or os.environ[name].strip() == ""
    ]
    if missing:
        raise RuntimeError(
            "Missing required S3 environment variables: " + ", ".join(missing)
        )
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name=os.environ.get("CORPSCOUT_S3_REGION", "us-east-1"),
    )


def _s3_summary(s3_client: Any, *, bucket: str, prefix: str) -> S3Summary:
    raw_report_count = 0
    done_marker_count = 0
    failed_marker_count = 0
    other_count = 0
    latest_key: str | None = None
    latest_modified: datetime | None = None
    failed_marker_keys: list[str] = []

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            modified = item.get("LastModified")
            if modified is not None and (
                latest_modified is None or modified > latest_modified
            ):
                latest_modified = modified
                latest_key = key

            kind = s3_key_kind(key)
            if kind == "raw_report":
                raw_report_count += 1
            elif kind == "done_marker":
                done_marker_count += 1
            elif kind == "failed_marker":
                failed_marker_count += 1
                failed_marker_keys.append(key)
            else:
                other_count += 1

    return S3Summary(
        bucket=bucket,
        prefix=prefix,
        total_count=raw_report_count
        + done_marker_count
        + failed_marker_count
        + other_count,
        raw_report_count=raw_report_count,
        done_marker_count=done_marker_count,
        failed_marker_count=failed_marker_count,
        other_count=other_count,
        latest_key=latest_key,
        latest_modified=latest_modified.isoformat() if latest_modified else None,
        failed_marker_keys=failed_marker_keys,
    )


def s3_key_kind(key: str) -> str:
    if key.endswith(DONE_MARKER_SUFFIX):
        return "done_marker"
    if key.endswith(FAILED_MARKER_SUFFIX):
        return "failed_marker"
    if "/year=" in key and "/type=" in key and "/id=" in key and key.endswith(".json"):
        return "raw_report"
    return "other"


def _read_json_object(s3_client: Any, *, bucket: str, key: str) -> dict[str, Any]:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def _print_s3_summary(label: str, summary: S3Summary) -> None:
    print(f"{label}:")
    print(f"  bucket={summary.bucket}")
    print(f"  prefix={summary.prefix}")
    print(
        "  counts="
        f"raw_reports={summary.raw_report_count} "
        f"done={summary.done_marker_count} "
        f"failed={summary.failed_marker_count} "
        f"other={summary.other_count} "
        f"total={summary.total_count}"
    )
    print(f"  latest={summary.latest_modified or '-'} {summary.latest_key or '-'}")


def _s3_progress_count(summary: S3Summary) -> int:
    return summary.raw_report_count + summary.done_marker_count + summary.failed_marker_count


if __name__ == "__main__":
    raise SystemExit(main())
