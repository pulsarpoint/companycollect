import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from norway_financial_bootstrap.activities import storage_from_env
from norway_financial_bootstrap.candidates import (
    build_financial_candidates,
    missing_candidates,
)
from norway_financial_bootstrap.workflows import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_CONCURRENT_BATCHES,
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    BootstrapInput,
    NorwayBrregInitialFinancialRawFetchWorkflow,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="norway-financial-bootstrap",
        description="Start the one-time Norway BRREG financial raw-fetch bootstrap workflow.",
    )
    parser.add_argument(
        "--snapshot-date",
        required=True,
        help="Snapshot date for the no_companies parquet, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--no-companies-key",
        required=True,
        help="S3 object key for the no_companies parquet snapshot.",
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
        "--task-queue",
        default=DEFAULT_TASK_QUEUE,
        help=f"Temporal task queue for the bootstrap worker. Default: {DEFAULT_TASK_QUEUE}.",
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Candidates per fetch activity. Default: {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--max-concurrent-batches",
        type=_positive_int,
        default=DEFAULT_MAX_CONCURRENT_BATCHES,
        help=(
            "Maximum fetch batch activities scheduled at once. "
            f"Default: {DEFAULT_MAX_CONCURRENT_BATCHES}."
        ),
    )
    return parser


async def start_workflow(
    *,
    temporal_address: str,
    task_queue: str,
    workflow_id: str,
    input: BootstrapInput,
) -> str:
    client = await Client.connect(temporal_address)
    handle = await client.start_workflow(
        NorwayBrregInitialFinancialRawFetchWorkflow.run,
        input,
        id=workflow_id,
        task_queue=task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.id


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    storage = storage_from_env()
    all_candidates = build_financial_candidates(storage.read_parquet(args.no_companies_key))
    candidates = missing_candidates(
        all_candidates,
        storage.existing_raw_fetch_org_years(),
    )
    workflow_id = f"norway-brreg-financial-raw-fetch-{args.snapshot_date}"
    temporal_address = args.temporal_address or os.environ.get(
        "TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS
    )
    workflow_input = BootstrapInput(
        source_run_id=workflow_id,
        fetched_at=_utc_now_iso(),
        batch_size=args.batch_size,
        candidates=candidates,
        max_concurrent_batches=args.max_concurrent_batches,
    )
    started_workflow_id = asyncio.run(
        start_workflow(
            temporal_address=temporal_address,
            task_queue=args.task_queue,
            workflow_id=workflow_id,
            input=workflow_input,
        )
    )
    sys.stdout.write(f"{started_workflow_id}\n")
    return 0


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
