import argparse
import asyncio
import concurrent.futures
import logging
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from norway_financial_bootstrap.activities import (
    candidate_marker_status,
    fetch_and_store_candidate,
    get_candidate,
    write_done_marker,
    write_failed_marker,
)
from norway_financial_bootstrap.workflows import (
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    NorwayBrregFinancialBootstrapSlotWorkflow,
)

DEFAULT_MAX_WORKERS = 4

logger = logging.getLogger("norway_financial_bootstrap.worker")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="norway-financial-bootstrap-worker",
        description="Run the Norway BRREG financial raw-fetch bootstrap Temporal worker.",
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


def build_worker(client: object, *, task_queue: str, max_workers: int) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[NorwayBrregFinancialBootstrapSlotWorkflow],
        activities=[
            get_candidate,
            candidate_marker_status,
            fetch_and_store_candidate,
            write_done_marker,
            write_failed_marker,
        ],
        activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=max_workers),
        max_concurrent_activities=max_workers,
    )


async def run_worker(
    *, temporal_address: str, task_queue: str, max_workers: int
) -> None:
    client = await Client.connect(temporal_address)
    worker = build_worker(client, task_queue=task_queue, max_workers=max_workers)
    logger.info(
        "connected to Temporal at %s | task queue %r | max_workers=%d",
        temporal_address,
        task_queue,
        max_workers,
    )
    await worker.run()


def worker_main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_dotenv(".env", override=False)
    if args.s3_endpoint is not None:
        os.environ["CORPSCOUT_S3_ENDPOINT"] = args.s3_endpoint
    logging.basicConfig(
        level=os.environ.get("NORWAY_FINANCIAL_BOOTSTRAP_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    temporal_address = args.temporal_address or os.environ.get(
        "TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS
    )
    try:
        asyncio.run(
            run_worker(
                temporal_address=temporal_address,
                task_queue=DEFAULT_TASK_QUEUE,
                max_workers=DEFAULT_MAX_WORKERS,
            )
        )
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(worker_main())
