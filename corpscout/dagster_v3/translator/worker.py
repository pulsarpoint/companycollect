# translator/worker.py
"""Translator Temporal worker fleet.

Starts TWO workers in one process:
  - a BUILD worker (BUILD_TASK_QUEUE): all source workflows + the
    seed / dump / summarize / start-translate handoff activities, normal concurrency;
  - an LLM worker (LLM_TASK_QUEUE): ONLY translate_loop_activity, bounded to K
    concurrent — the single global LLM gate shared by every source.

.env loading uses python-dotenv (load_dotenv(..., override=False) — shell / docker -e win).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from translator.task_queues import BUILD_TASK_QUEUE, LLM_TASK_QUEUE
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    TranslateWorkflow,
    build_queue_activity,
    dump_activity,
    start_translate_workflow_activity,
    summarize_queue_activity,
    translate_loop_activity,
)

logger = logging.getLogger("translator.worker")

DEFAULT_LLM_CONCURRENCY = 2


def llm_concurrency() -> int:
    """Global LLM gate size K, from TRANSLATOR_LLM_CONCURRENCY (default 2)."""
    return int(os.environ.get("TRANSLATOR_LLM_CONCURRENCY", str(DEFAULT_LLM_CONCURRENCY)))


def build_build_worker(client: object) -> Worker:
    """Worker for the workflows + build/dump/summarize/handoff activities."""
    return Worker(
        client,
        task_queue=BUILD_TASK_QUEUE,
        workflows=[BuildQueueWorkflow, TranslateWorkflow],
        activities=[
            build_queue_activity,
            start_translate_workflow_activity,
            dump_activity,
            summarize_queue_activity,
        ],
    )


def build_llm_worker(client: object, *, max_concurrent: int) -> Worker:
    """Worker that runs ONLY translate_loop_activity, capped at max_concurrent (global gate)."""
    return Worker(
        client,
        task_queue=LLM_TASK_QUEUE,
        activities=[translate_loop_activity],
        max_concurrent_activities=max_concurrent,
    )


async def run_worker(temporal_address: str | None = None) -> None:
    address = temporal_address or os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    client = await Client.connect(address)
    k = llm_concurrency()
    logger.info(
        "connected to Temporal at %s | build queue %r, llm queue %r (K=%d) (Ctrl-C to stop)",
        address,
        BUILD_TASK_QUEUE,
        LLM_TASK_QUEUE,
        k,
    )
    build_worker = build_build_worker(client)
    llm_worker = build_llm_worker(client, max_concurrent=k)
    await asyncio.gather(build_worker.run(), llm_worker.run())


def worker_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="translator-worker",
        description="Run the standalone translator Temporal worker fleet.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a .env file to load before starting (default: .env).",
    )
    parser.add_argument(
        "--temporal-address",
        default=None,
        help="Temporal frontend address (overrides TEMPORAL_ADDRESS).",
    )
    args = parser.parse_args(argv)
    load_dotenv(args.env_file, override=False)
    logging.basicConfig(
        level=os.environ.get("TRANSLATOR_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    logger.info("starting translator worker fleet")
    try:
        asyncio.run(run_worker(temporal_address=args.temporal_address))
    except KeyboardInterrupt:
        return 130
    return 0
