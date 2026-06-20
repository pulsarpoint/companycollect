from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
from uuid import uuid4

from temporalio.client import Client
from temporalio.worker import Worker

from temporal.translations.queue import (
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    TranslationQueueWorkflow,
    TranslationQueueWorkflowInput,
    initialize_translation_queue,
    process_translation_batch,
    summarize_translation_queue,
)
from translations.provider_smoke import _load_env_file


def build_worker_config() -> dict[str, object]:
    return {
        "task_queue": LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        "max_concurrent_activities": 1,
    }


def parse_start_args(argv: list[str] | None = None) -> tuple[str, TranslationQueueWorkflowInput]:
    parser = argparse.ArgumentParser(description="Start a Temporal translation queue workflow.")
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--batch-size", default=50, type=int)
    parser.add_argument("--timeout-seconds", default=120, type=int)
    parser.add_argument("--max-batch-failures", default=0, type=int)
    parser.add_argument("--worker-id", default="translation-temporal-worker")
    parser.add_argument("--initialize-timeout-seconds", default=300, type=int)
    parser.add_argument("--batch-timeout-buffer-seconds", default=600, type=int)
    parser.add_argument("--summarize-timeout-seconds", default=30, type=int)
    parser.add_argument("--activity-maximum-attempts", default=1, type=int)
    parser.add_argument("--lease-timeout-seconds", default=1800, type=int)
    parser.add_argument(
        "--max-tokens",
        default=int(os.getenv("TRANSLATION_PROVIDER_MAX_TOKENS", "2048")),
        type=int,
    )
    parser.add_argument(
        "--extra-body-json",
        default=os.getenv(
            "TRANSLATION_PROVIDER_EXTRA_BODY_JSON",
            '{"chat_template_kwargs":{"enable_thinking":false}}',
        ),
    )
    parser.add_argument("--workflow-id")
    args = parser.parse_args(argv)

    workflow_id = args.workflow_id or f"translation-{args.source}-{uuid4()}"
    return workflow_id, TranslationQueueWorkflowInput(
        duckdb_path=args.duckdb_path,
        batch_size=args.batch_size,
        timeout_seconds=args.timeout_seconds,
        max_batch_failures=args.max_batch_failures,
        worker_id=args.worker_id,
        max_tokens=args.max_tokens,
        extra_body_json=args.extra_body_json,
        initialize_timeout_seconds=args.initialize_timeout_seconds,
        batch_timeout_buffer_seconds=args.batch_timeout_buffer_seconds,
        summarize_timeout_seconds=args.summarize_timeout_seconds,
        activity_maximum_attempts=args.activity_maximum_attempts,
        lease_timeout_seconds=args.lease_timeout_seconds,
    )


async def run_worker(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the serialized local LLM Temporal worker.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--temporal-address", default=os.getenv("TEMPORAL_ADDRESS"))
    args = parser.parse_args(argv)

    _load_env_file(Path(args.env_file))
    temporal_address = args.temporal_address or os.environ["TEMPORAL_ADDRESS"]
    client = await Client.connect(temporal_address)
    config = build_worker_config()
    async with Worker(
        client,
        task_queue=str(config["task_queue"]),
        workflows=[TranslationQueueWorkflow],
        activities=[
            initialize_translation_queue,
            process_translation_batch,
            summarize_translation_queue,
        ],
        max_concurrent_activities=int(config["max_concurrent_activities"]),
    ):
        print(
            json.dumps(
                {
                    "event": "translation_temporal_worker_started",
                    "max_concurrent_activities": config["max_concurrent_activities"],
                    "task_queue": config["task_queue"],
                    "temporal_address": temporal_address,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        await asyncio.Event().wait()


async def start_workflow(argv: list[str] | None = None) -> dict[str, object]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--temporal-address", default=os.getenv("TEMPORAL_ADDRESS"))
    known_args, remaining = parser.parse_known_args(argv)

    _load_env_file(Path(known_args.env_file))
    temporal_address = known_args.temporal_address or os.environ["TEMPORAL_ADDRESS"]
    workflow_id, params = parse_start_args(remaining)
    client = await Client.connect(temporal_address)
    handle = await client.start_workflow(
        TranslationQueueWorkflow.run,
        params,
        id=workflow_id,
        task_queue=LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    )
    result = {
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
        "task_queue": LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        "input": asdict(params),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


async def status(argv: list[str] | None = None) -> dict[str, object]:
    parser = argparse.ArgumentParser(description="Read translation queue and optional Temporal status.")
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument("--workflow-id")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--temporal-address", default=os.getenv("TEMPORAL_ADDRESS"))
    args = parser.parse_args(argv)

    _load_env_file(Path(args.env_file))
    from temporal.translations.queue import summarize_translation_queue_once

    result: dict[str, object] = {
        "queue": summarize_translation_queue_once(args.duckdb_path),
    }
    if args.workflow_id:
        temporal_address = args.temporal_address or os.environ["TEMPORAL_ADDRESS"]
        client = await Client.connect(temporal_address)
        description = await client.get_workflow_handle(args.workflow_id).describe()
        result["temporal"] = {
            "workflow_id": args.workflow_id,
            "status": description.status.name,
            "run_id": description.run_id,
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def worker_main() -> int:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        return 130
    return 0


def start_main() -> int:
    asyncio.run(start_workflow())
    return 0


def status_main() -> int:
    asyncio.run(status())
    return 0
