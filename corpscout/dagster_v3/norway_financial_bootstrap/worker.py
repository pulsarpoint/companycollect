import argparse
import asyncio
import concurrent.futures
import logging
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from norway_financial_bootstrap.activities import fetch_batch
from norway_financial_bootstrap.workflows import (
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    NorwayBrregInitialFinancialRawFetchWorkflow,
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
        "--task-queue",
        default=DEFAULT_TASK_QUEUE,
        help=f"Temporal task queue to poll. Default: {DEFAULT_TASK_QUEUE}.",
    )
    parser.add_argument(
        "--max-workers",
        type=_positive_int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Maximum concurrent fetch activities. Default: {DEFAULT_MAX_WORKERS}.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a .env file to load before starting. Default: .env.",
    )
    return parser


def build_worker(client: object, *, task_queue: str, max_workers: int) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[NorwayBrregInitialFinancialRawFetchWorkflow],
        activities=[fetch_batch],
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
    load_dotenv(args.env_file, override=False)
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
                task_queue=args.task_queue,
                max_workers=args.max_workers,
            )
        )
    except KeyboardInterrupt:
        return 130
    return 0


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


if __name__ == "__main__":
    raise SystemExit(worker_main())
