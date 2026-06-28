"""BuildQueueWorkflow + TranslateWorkflow for Norway Brreg translations.

BuildQueueWorkflow:
  1. build_queue_activity  — Arrow seed → DuckDB queue (heartbeating)
  2. start_translate_workflow_activity — fires TranslateWorkflow (USE_EXISTING),
     then BuildQueueWorkflow COMPLETES.

TranslateWorkflow:
  1. translate_loop_activity — long heartbeating LLM drain loop
  2. dump_activity           — queue → corpscout.text_translations (batched, heartbeating)
  3. summarize_queue_activity — read final queue summary
"""
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy

from translator.task_queues import LLM_TASK_QUEUE

# SeedResult is a pure dataclass (no I/O at import time) — safe to pass through.
with workflow.unsafe.imports_passed_through():
    from translator.norway_brreg.seed import SeedResult

logger = logging.getLogger("translator.norway_brreg.workflows")

HEARTBEAT_TIMEOUT = timedelta(seconds=150)
START_TO_CLOSE_TIMEOUT = timedelta(hours=24)
SHORT_TIMEOUT = timedelta(seconds=60)
RETRY_POLICY = RetryPolicy(maximum_attempts=3)

# ---------------------------------------------------------------------------
# Input / Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BuildQueueActivityInput:
    source_slug: str
    queue_duckdb_path: str


@dataclass(frozen=True)
class StartTranslateWorkflowInput:
    workflow_id: str
    task_queue: str
    source_slug: str
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class TranslateLoopActivityInput:
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class TranslateLoopResult:
    completed_items: int
    failed_batches: int
    successful_batches: int


@dataclass(frozen=True)
class DumpActivityInput:
    source_slug: str
    queue_duckdb_path: str


@dataclass(frozen=True)
class BuildQueueWorkflowInput:
    source_slug: str
    queue_duckdb_path: str
    translate_workflow_id: str
    translate_task_queue: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class BuildQueueWorkflowOutput:
    dynamic_enqueued: int
    static_flushed: int


@dataclass(frozen=True)
class TranslateWorkflowInput:
    source_slug: str
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class TranslateWorkflowOutput:
    completed_items: int
    failed_retryable_items: int
    flushed_rows: int
    successful_batches: int
    failed_batches: int


# ---------------------------------------------------------------------------
# Activity sync implementations (_once functions for monkeypatching in tests)
# ---------------------------------------------------------------------------


def build_queue_once(params: BuildQueueActivityInput) -> SeedResult:
    from translator.clickhouse import clickhouse_client_from_env
    from translator.norway_brreg.config import get_config
    from translator.norway_brreg.seed import build_queue

    config = get_config()
    ch_client = clickhouse_client_from_env()
    try:
        return build_queue(
            config,
            ch_client,
            params.queue_duckdb_path,
            heartbeat_fn=activity.heartbeat,
        )
    finally:
        close = getattr(ch_client, "close", None)
        if callable(close):
            close()


def translate_loop_once(params: TranslateLoopActivityInput):
    from translator.errors import _categorize_exception
    from translator.llm_batch import translate_batch
    from translator.provider import (
        LocalOpenAICompatibleTranslationProvider,
        _parse_extra_body,
    )
    from translator.queue import TranslationQueue

    queue = TranslationQueue(params.queue_duckdb_path)
    queue.initialize()

    model = os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"]
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
        model=model,
        api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
        max_tokens=params.max_tokens,
        extra_body=_parse_extra_body(params.extra_body_json),
    )
    success_count = 0
    failure_count = 0
    completed_items = 0
    last_heartbeat_at = time.time()

    try:
        while True:
            now = time.time()
            if now - last_heartbeat_at >= 30:
                activity.heartbeat(completed_items)
                last_heartbeat_at = now

            claimed = queue.claim_batch(limit=params.batch_size, worker_id="translate-worker")
            if not claimed:
                break

            started_at = time.perf_counter()
            try:
                results = translate_batch(claimed, provider=provider, timeout=120)
                duration = time.perf_counter() - started_at
                queue.complete_batch(
                    claimed,
                    results,
                    provider=type(provider).__name__,
                    model=model,
                    duration_seconds=duration,
                )
                success_count += 1
                completed_items += len(claimed)
                activity.heartbeat(completed_items)
                last_heartbeat_at = time.time()
            except Exception as exc:
                duration = time.perf_counter() - started_at
                error_category = _categorize_exception(exc)
                queue.fail_batch(
                    claimed,
                    error_category=error_category,
                    error_message=str(exc),
                    duration_seconds=duration,
                )
                failure_count += 1
                logger.warning(
                    "translate_loop: batch failed (%s): %s", error_category, exc
                )
                if params.max_batch_failures > 0 and failure_count >= params.max_batch_failures:
                    logger.warning(
                        "translate_loop: exceeded max_batch_failures=%d, stopping",
                        params.max_batch_failures,
                    )
                    break
    finally:
        provider.close()

    return TranslateLoopResult(
        completed_items=completed_items,
        failed_batches=failure_count,
        successful_batches=success_count,
    )


def dump_once(params: DumpActivityInput):
    from translator.clickhouse import clickhouse_client_from_env
    from translator.norway_brreg.config import get_config
    from translator.norway_brreg.dump import dump_to_clickhouse

    # Resolve the model from env inside the activity (env access is not allowed
    # in the workflow sandbox); dumped rows are labelled provider='local-llm'.
    model = os.environ.get("TRANSLATION_PROVIDER_LOCAL_MODEL", "local-llm")
    config = get_config()
    ch_client = clickhouse_client_from_env()
    try:
        return dump_to_clickhouse(
            params.queue_duckdb_path,
            ch_client,
            config,
            provider="local-llm",
            model=model,
            heartbeat_fn=activity.heartbeat,
        )
    finally:
        close = getattr(ch_client, "close", None)
        if callable(close):
            close()


def summarize_queue_once(queue_duckdb_path: str) -> dict:
    from translator.queue import TranslationQueue

    s = TranslationQueue(queue_duckdb_path).summary()
    return {
        "total_items": s.total_items,
        "completed_items": s.completed_items,
        "failed_retryable_items": s.failed_retryable_items,
        "pending_items": s.pending_items,
    }


# ---------------------------------------------------------------------------
# Activity definitions
# ---------------------------------------------------------------------------


@activity.defn
def build_queue_activity(params: BuildQueueActivityInput) -> SeedResult:
    return build_queue_once(params)


@activity.defn
async def start_translate_workflow_activity(params: StartTranslateWorkflowInput) -> str:
    address = os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    # temporalio Client has no close() method or async-context-manager support;
    # the per-call connect is intentional — this activity runs once per BuildQueue run.
    client = await Client.connect(address)
    handle = await client.start_workflow(
        TranslateWorkflow.run,
        TranslateWorkflowInput(
            source_slug=params.source_slug,
            queue_duckdb_path=params.queue_duckdb_path,
            batch_size=params.batch_size,
            max_tokens=params.max_tokens,
            extra_body_json=params.extra_body_json,
            max_batch_failures=params.max_batch_failures,
        ),
        id=params.workflow_id,
        task_queue=params.task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.id


@activity.defn
def translate_loop_activity(params: TranslateLoopActivityInput) -> TranslateLoopResult:
    return translate_loop_once(params)


@activity.defn
def dump_activity(params: DumpActivityInput) -> int:
    return dump_once(params)


@activity.defn
def summarize_queue_activity(queue_duckdb_path: str) -> dict:
    return summarize_queue_once(queue_duckdb_path)


# ---------------------------------------------------------------------------
# Workflow definitions
# ---------------------------------------------------------------------------


@workflow.defn
class BuildQueueWorkflow:
    """Bulk-seed the DuckDB queue from ClickHouse, then hand off to TranslateWorkflow."""

    @workflow.run
    async def run(self, params: BuildQueueWorkflowInput) -> BuildQueueWorkflowOutput:
        seed_result: SeedResult = await workflow.execute_activity(
            build_queue_activity,
            BuildQueueActivityInput(
                source_slug=params.source_slug,
                queue_duckdb_path=params.queue_duckdb_path,
            ),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            start_to_close_timeout=START_TO_CLOSE_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        await workflow.execute_activity(
            start_translate_workflow_activity,
            StartTranslateWorkflowInput(
                workflow_id=params.translate_workflow_id,
                task_queue=params.translate_task_queue,
                source_slug=params.source_slug,
                queue_duckdb_path=params.queue_duckdb_path,
                batch_size=params.batch_size,
                max_tokens=params.max_tokens,
                extra_body_json=params.extra_body_json,
                max_batch_failures=params.max_batch_failures,
            ),
            start_to_close_timeout=SHORT_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        return BuildQueueWorkflowOutput(
            dynamic_enqueued=seed_result.dynamic_enqueued,
            static_flushed=seed_result.static_flushed,
        )


@workflow.defn
class TranslateWorkflow:
    """Drain the DuckDB queue via LLM, then dump to corpscout.text_translations."""

    @workflow.run
    async def run(self, params: TranslateWorkflowInput) -> TranslateWorkflowOutput:
        # Route the LLM drain loop to the gated LLM queue. NO schedule_to_start_timeout:
        # the loop waits in the queue until one of the K global slots frees up.
        loop_result: TranslateLoopResult = await workflow.execute_activity(
            translate_loop_activity,
            TranslateLoopActivityInput(
                queue_duckdb_path=params.queue_duckdb_path,
                batch_size=params.batch_size,
                max_tokens=params.max_tokens,
                extra_body_json=params.extra_body_json,
                max_batch_failures=params.max_batch_failures,
            ),
            task_queue=LLM_TASK_QUEUE,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            start_to_close_timeout=START_TO_CLOSE_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        # dump + summarize stay on the workflow's own (build) task queue.
        flushed: int = await workflow.execute_activity(
            dump_activity,
            DumpActivityInput(
                source_slug=params.source_slug,
                queue_duckdb_path=params.queue_duckdb_path,
            ),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            start_to_close_timeout=START_TO_CLOSE_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        summary: dict = await workflow.execute_activity(
            summarize_queue_activity,
            params.queue_duckdb_path,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RETRY_POLICY,
        )

        return TranslateWorkflowOutput(
            completed_items=summary["completed_items"],
            failed_retryable_items=summary["failed_retryable_items"],
            flushed_rows=flushed,
            successful_batches=loop_result.successful_batches,
            failed_batches=loop_result.failed_batches,
        )
