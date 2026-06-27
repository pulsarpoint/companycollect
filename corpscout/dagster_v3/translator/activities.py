from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
import os
import time

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from translator.types import SmokeTranslationResult

logger = logging.getLogger("translator.activities")

LOCAL_LLM_TRANSLATION_TASK_QUEUE = "translation-local-llm"


@dataclass(frozen=True)
class TranslationQueueWorkflowInput:
    duckdb_path: str
    batch_size: int
    timeout_seconds: int
    max_batch_failures: int
    worker_id: str
    max_tokens: int
    extra_body_json: str
    initialize_timeout_seconds: int
    batch_timeout_buffer_seconds: int
    summarize_timeout_seconds: int
    activity_maximum_attempts: int
    lease_timeout_seconds: int


@dataclass(frozen=True)
class TranslationQueueWorkflowOutput:
    total_items: int
    location_items: int
    completed_items: int
    failed_retryable_items: int
    successful_batches: int
    failed_batches: int
    provider_success_count: int
    provider_failure_count: int


@dataclass(frozen=True)
class InitializeTranslationQueueInput:
    duckdb_path: str


@dataclass(frozen=True)
class ProcessTranslationBatchInput:
    duckdb_path: str
    batch_size: int
    timeout_seconds: int
    worker_id: str
    max_tokens: int
    extra_body_json: str
    lease_timeout_seconds: int


@dataclass(frozen=True)
class ProcessTranslationBatchResult:
    status: str
    item_count: int
    duration_seconds: float
    error_category: str | None = None
    error_message: str | None = None


def initialize_translation_queue_once(params: InitializeTranslationQueueInput) -> int:
    from translator.queue import TranslationQueue

    queue = TranslationQueue(params.duckdb_path)
    queue.initialize()
    return queue.summary().total_items


def process_translation_batch_once(
    params: ProcessTranslationBatchInput,
    *,
    provider: object | None = None,
) -> ProcessTranslationBatchResult:
    from translator.provider_smoke import _categorize_exception, _parse_extra_body
    from translator.queue import TranslationQueue
    from translator.queue_smoke import (
        _map_provider_results_to_queue_ids,
        _provider_inputs,
    )
    from translator.smoke import LocalOpenAICompatibleTranslationProvider

    queue = TranslationQueue(params.duckdb_path)
    queue.initialize()
    queue.release_stale_leases(older_than_seconds=params.lease_timeout_seconds)
    model = (
        os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"]
        if provider is None
        else str(getattr(provider, "model", type(provider).__name__))
    )
    claimed = queue.claim_batch(limit=params.batch_size, worker_id=params.worker_id)
    if not claimed:
        logger.info("process_batch: queue drained (no pending items)")
        return ProcessTranslationBatchResult(
            status="empty",
            item_count=0,
            duration_seconds=0.0,
        )
    logger.info("process_batch: claimed %d item(s) (model=%s)", len(claimed), model)

    provider_inputs = _provider_inputs(claimed)
    queue_id_by_provider_id = {
        provider_input.item_id: claimed_item.item_id
        for provider_input, claimed_item in zip(provider_inputs, claimed, strict=True)
    }
    close_provider = provider is None
    if provider is None:
        active_provider = LocalOpenAICompatibleTranslationProvider(
            base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
            model=model,
            api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
            max_tokens=params.max_tokens,
            extra_body=_parse_extra_body(params.extra_body_json),
        )
    else:
        active_provider = provider

    started_at = time.perf_counter()
    try:
        translations = active_provider.translate(
            provider_inputs,
            timeout_seconds=params.timeout_seconds,
        )
    except Exception as exc:
        duration_seconds = _elapsed_seconds(started_at)
        error_category = _categorize_exception(exc)
        error_message = str(exc)
        if close_provider:
            active_provider.close()
        queue.fail_batch(
            claimed,
            error_category=error_category,
            error_message=error_message,
            duration_seconds=duration_seconds,
        )
        logger.warning(
            "process_batch: batch FAILED (%s) for %d item(s): %s",
            error_category,
            len(claimed),
            error_message,
        )
        return ProcessTranslationBatchResult(
            status="failed",
            item_count=len(claimed),
            duration_seconds=duration_seconds,
            error_category=error_category,
            error_message=error_message,
        )

    duration_seconds = _elapsed_seconds(started_at)
    if close_provider:
        active_provider.close()
    queue.complete_batch(
        claimed,
        _map_provider_results_to_queue_ids(
            translations,
            queue_id_by_provider_id=queue_id_by_provider_id,
        ),
        provider=type(active_provider).__name__,
        model=model,
        duration_seconds=duration_seconds,
    )
    logger.info("process_batch: translated %d item(s) in %.1fs", len(claimed), duration_seconds)
    return ProcessTranslationBatchResult(
        status="success",
        item_count=len(claimed),
        duration_seconds=duration_seconds,
    )


@activity.defn
async def initialize_translation_queue(params: InitializeTranslationQueueInput) -> int:
    return await asyncio.to_thread(initialize_translation_queue_once, params)


@activity.defn
async def process_translation_batch(
    params: ProcessTranslationBatchInput,
) -> ProcessTranslationBatchResult:
    return await asyncio.to_thread(process_translation_batch_once, params)


@workflow.defn
class TranslationQueueWorkflow:
    @workflow.run
    async def run(self, params: TranslationQueueWorkflowInput) -> TranslationQueueWorkflowOutput:
        await workflow.execute_activity(
            initialize_translation_queue,
            InitializeTranslationQueueInput(duckdb_path=params.duckdb_path),
            start_to_close_timeout=timedelta(seconds=params.initialize_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )

        provider_success_count = 0
        provider_failure_count = 0
        while True:
            result = await workflow.execute_activity(
                process_translation_batch,
                ProcessTranslationBatchInput(
                    duckdb_path=params.duckdb_path,
                    batch_size=params.batch_size,
                    timeout_seconds=params.timeout_seconds,
                    worker_id=params.worker_id,
                    max_tokens=params.max_tokens,
                    extra_body_json=params.extra_body_json,
                    lease_timeout_seconds=params.lease_timeout_seconds,
                ),
                start_to_close_timeout=timedelta(
                    seconds=params.timeout_seconds + params.batch_timeout_buffer_seconds
                ),
                retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
            )
            if result.status == "empty":
                break
            if result.status == "success":
                provider_success_count += 1
                continue
            provider_failure_count += 1
            if params.max_batch_failures > 0 and provider_failure_count > params.max_batch_failures:
                break

        summary = await workflow.execute_activity(
            summarize_translation_queue,
            params.duckdb_path,
            start_to_close_timeout=timedelta(seconds=params.summarize_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )
        return TranslationQueueWorkflowOutput(
            total_items=summary["total_items"],
            location_items=summary["location_items"],
            completed_items=summary["completed_items"],
            failed_retryable_items=summary["failed_retryable_items"],
            successful_batches=summary["successful_batches"],
            failed_batches=summary["failed_batches"],
            provider_success_count=provider_success_count,
            provider_failure_count=provider_failure_count,
        )


@activity.defn
async def summarize_translation_queue(duckdb_path: str) -> dict[str, int]:
    return await asyncio.to_thread(summarize_translation_queue_once, duckdb_path)


def summarize_translation_queue_once(duckdb_path: str) -> dict[str, int]:
    from translator.queue import TranslationQueue

    summary = TranslationQueue(duckdb_path).summary()
    return {
        "total_items": summary.total_items,
        "location_items": summary.location_items,
        "pending_items": summary.pending_items,
        "leased_items": summary.leased_items,
        "completed_items": summary.completed_items,
        "failed_retryable_items": summary.failed_retryable_items,
        "result_items": summary.result_items,
        "successful_batches": summary.successful_batches,
        "failed_batches": summary.failed_batches,
    }


def _elapsed_seconds(started_at: float) -> float:
    return round(time.perf_counter() - started_at, 3)
