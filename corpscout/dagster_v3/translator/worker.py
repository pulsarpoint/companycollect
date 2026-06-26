from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

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


def load_env_file(path: Path) -> int:
    """Load KEY=VALUE lines from a .env file into os.environ.

    Existing environment variables are NOT overridden (shell / `docker -e` win),
    so the file is a fallback. Blank lines and `#` comments are ignored, an
    optional leading `export ` is stripped, and matching surrounding quotes are
    removed. Returns the number of variables actually set.
    """
    if not path.is_file():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if not key or key in os.environ:
            continue
        os.environ[key] = value
        loaded += 1
    return loaded


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


def worker_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="translator-worker",
        description="Run the standalone translator Temporal worker.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a .env file to load before starting (default: .env in the "
        "current directory). Existing environment variables are not overridden.",
    )
    parser.add_argument(
        "--temporal-address",
        default=None,
        help="Temporal frontend address (overrides TEMPORAL_ADDRESS / the default).",
    )
    args = parser.parse_args(argv)

    load_env_file(Path(args.env_file))

    try:
        asyncio.run(run_worker(temporal_address=args.temporal_address))
    except KeyboardInterrupt:
        return 130
    return 0
