from __future__ import annotations

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from translator.activities import (
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    process_translation_batch,
    summarize_translation_queue,
)
from translator.workflow import (
    TranslateSourceWorkflow,
    flush_activity,
    scan_and_seed_activity,
)


def build_worker(client: object) -> Worker:
    return Worker(
        client,
        task_queue=LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        workflows=[TranslateSourceWorkflow],
        activities=[
            scan_and_seed_activity,
            flush_activity,
            process_translation_batch,
            summarize_translation_queue,
        ],
    )


async def run_worker(temporal_address: str | None = None) -> None:
    address = temporal_address or os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    client = await Client.connect(address)
    await build_worker(client).run()


def worker_main() -> int:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        return 130
    return 0
