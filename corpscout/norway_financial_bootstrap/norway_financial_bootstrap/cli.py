import argparse
import asyncio
import logging
import os
import sys
from datetime import UTC, datetime

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from norway_financial_bootstrap.clickhouse import (
    clickhouse_from_env,
    prepare_candidate_table,
)
from norway_financial_bootstrap.workflows import (
    DEFAULT_SLOT_COUNT,
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    BootstrapSlotInput,
    NorwayBrregFinancialBootstrapSlotWorkflow,
    slot_workflow_id,
)

FIXED_WORKFLOW_ID = "norway-brreg-finance-historical-bootstrap"
logger = logging.getLogger("norway_financial_bootstrap.starter")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="norway-financial-bootstrap",
        description="Start the Norway BRREG financial raw-fetch slot workflows.",
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


async def start_slot_workflows(
    *,
    client: Client,
    run_id: str,
    fetched_at: str,
    slot_count: int,
    task_queue: str,
) -> list[str]:
    workflow_ids: list[str] = []
    for slot_id in range(slot_count):
        workflow_id = slot_workflow_id(run_id, slot_id)
        try:
            handle = await client.start_workflow(
                NorwayBrregFinancialBootstrapSlotWorkflow.run,
                BootstrapSlotInput(
                    run_id=run_id,
                    slot_id=slot_id,
                    slot_index=0,
                    slot_count=slot_count,
                    fetched_at=fetched_at,
                ),
                id=workflow_id,
                task_queue=task_queue,
            )
            workflow_ids.append(handle.id)
        except WorkflowAlreadyStartedError:
            workflow_ids.append(workflow_id)
    return workflow_ids


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv(".env", override=False)
    logging.basicConfig(
        level=os.environ.get("NORWAY_FINANCIAL_BOOTSTRAP_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    if args.s3_endpoint is not None:
        os.environ["CORPSCOUT_S3_ENDPOINT"] = args.s3_endpoint

    run_id = FIXED_WORKFLOW_ID
    slot_count = DEFAULT_SLOT_COUNT
    logger.info(
        "preparing Norway financial bootstrap candidate table: run_id=%s slot_count=%d",
        run_id,
        slot_count,
    )
    preparation = prepare_candidate_table(
        clickhouse_from_env(),
        run_id=run_id,
        slot_count=slot_count,
    )
    logger.info(
        "candidate table ready: run_id=%s inserted=%s existing_count=%d "
        "candidate_count=%d stored_slot_count=%d",
        preparation.run_id,
        preparation.inserted,
        preparation.existing_count,
        preparation.candidate_count,
        preparation.stored_slot_count,
    )

    temporal_address = args.temporal_address or os.environ.get(
        "TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS
    )
    started_workflow_ids = asyncio.run(
        _connect_and_start_slot_workflows(
            temporal_address=temporal_address,
            run_id=run_id,
            fetched_at=_utc_now_iso(),
            slot_count=slot_count,
            task_queue=DEFAULT_TASK_QUEUE,
        )
    )
    for workflow_id in started_workflow_ids:
        sys.stdout.write(f"{workflow_id}\n")
    return 0


async def _connect_and_start_slot_workflows(
    *,
    temporal_address: str,
    run_id: str,
    fetched_at: str,
    slot_count: int,
    task_queue: str,
) -> list[str]:
    client = await Client.connect(temporal_address)
    return await start_slot_workflows(
        client=client,
        run_id=run_id,
        fetched_at=fetched_at,
        slot_count=slot_count,
        task_queue=task_queue,
    )


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
