import argparse
import asyncio
import os
import sys
import uuid
from datetime import UTC, datetime

from dotenv import load_dotenv
from temporalio.client import Client

from norway_financial_bootstrap.activities import storage_from_env
from norway_financial_bootstrap.clickhouse import (
    clickhouse_from_env,
    financial_candidates_from_clickhouse,
)
from norway_financial_bootstrap.candidates import (
    FinancialCandidate,
)
from norway_financial_bootstrap.storage import NorwayFinancialBootstrapStorage
from norway_financial_bootstrap.workflows import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_CONCURRENT_BATCHES,
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    BootstrapInput,
    NorwayBrregInitialFinancialRawFetchWorkflow,
    partition_batches,
)

FIXED_WORKFLOW_ID = "norway-brreg-finance-historical-bootstrap"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="norway-financial-bootstrap",
        description="Start the one-time Norway BRREG financial raw-fetch bootstrap workflow.",
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
    )
    return handle.id


def write_candidate_batches(
    *,
    storage: NorwayFinancialBootstrapStorage,
    source_run_id: str,
    attempt_id: str,
    candidates: list[FinancialCandidate],
    batch_size: int,
) -> list[str]:
    return [
        storage.write_candidate_batch(source_run_id, attempt_id, batch_index, batch)
        for batch_index, batch in enumerate(
            partition_batches(candidates, batch_size=batch_size)
        )
    ]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv(".env", override=False)
    if args.s3_endpoint is not None:
        os.environ["CORPSCOUT_S3_ENDPOINT"] = args.s3_endpoint
    storage = storage_from_env()
    candidates = financial_candidates_from_clickhouse(clickhouse_from_env())
    workflow_id = FIXED_WORKFLOW_ID
    temporal_address = args.temporal_address or os.environ.get(
        "TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS
    )
    attempt_id = _generate_attempt_id()
    batch_keys = write_candidate_batches(
        storage=storage,
        source_run_id=workflow_id,
        attempt_id=attempt_id,
        candidates=candidates,
        batch_size=DEFAULT_BATCH_SIZE,
    )
    workflow_input = BootstrapInput(
        source_run_id=workflow_id,
        fetched_at=_utc_now_iso(),
        candidate_count=len(candidates),
        batch_keys=batch_keys,
        max_concurrent_batches=DEFAULT_MAX_CONCURRENT_BATCHES,
    )
    started_workflow_id = asyncio.run(
        start_workflow(
            temporal_address=temporal_address,
            task_queue=DEFAULT_TASK_QUEUE,
            workflow_id=workflow_id,
            input=workflow_input,
        )
    )
    sys.stdout.write(f"{started_workflow_id}\n")
    return 0


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _generate_attempt_id() -> str:
    return uuid.uuid4().hex


if __name__ == "__main__":
    raise SystemExit(main())
