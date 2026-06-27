from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from translator.activities import (
    ProcessTranslationBatchInput,
    process_translation_batch,
    summarize_translation_queue,
)

# Heavy deps (duckdb, clickhouse) are inside the activity helper bodies.
# Module-level names are defined here so tests can monkeypatch them; the
# sandbox passthrough ensures they aren't re-imported inside the sandbox.
with workflow.unsafe.imports_passed_through():
    from translator.clickhouse import clickhouse_client_from_env, scan_untranslated_terms
    from translator.flush import flush_translations
    from translator.registry import get_source_config


@dataclass(frozen=True)
class TranslateSourceWorkflowInput:
    source_slug: str
    queue_dir: str
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
    scan_timeout_seconds: int
    flush_timeout_seconds: int


@dataclass(frozen=True)
class TranslateSourceWorkflowOutput:
    enqueued_items: int
    completed_items: int
    failed_retryable_items: int
    flushed_rows: int


@dataclass(frozen=True)
class ScanAndSeedInput:
    source_slug: str
    queue_duckdb_path: str


@dataclass(frozen=True)
class ScanAndSeedResult:
    """Result returned by the scan-and-seed activity.

    ``dynamic_enqueued`` is the count of free-text terms placed on the DuckDB
    queue for LLM translation.  ``static_flushed`` is the count of static-map
    terms flushed directly to ``text_translations`` (bypassing the LLM queue).
    """

    dynamic_enqueued: int
    static_flushed: int


@dataclass(frozen=True)
class FlushInput:
    source_slug: str
    queue_duckdb_path: str
    version: int


def scan_and_seed_once(params: ScanAndSeedInput) -> ScanAndSeedResult:
    from translator.queue import FlushTranslationRow, TranslationQueue, TranslationQueueItem

    source_config = get_source_config(params.source_slug)
    field_by_col = {f.original_col: f for f in source_config.fields}

    client = clickhouse_client_from_env()
    static_flushed = 0
    try:
        terms = scan_untranslated_terms(client, source_config)

        static_terms = []
        dynamic_terms = []
        for term in terms:
            fc = field_by_col.get(term.source_column)
            if fc is not None and fc.static_map is not None:
                static_terms.append(term)
            else:
                dynamic_terms.append(term)

        if static_terms:
            static_rows: list[FlushTranslationRow] = []
            for term in static_terms:
                fc = field_by_col[term.source_column]
                translation = (fc.static_map_dict() or {}).get(term.static_key or "", "")
                if translation:
                    static_rows.append(
                        FlushTranslationRow(
                            source_column=term.source_column,
                            source_text=term.source_text,
                            translated_text=translation,
                        )
                    )
            if static_rows:
                static_flushed = flush_translations(
                    client,
                    source_config,
                    static_rows,
                    provider="static",
                    model="static",
                    version=int(time.time()),
                    run_id=params.queue_duckdb_path,
                )
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()

    queue = TranslationQueue(params.queue_duckdb_path)
    queue.initialize()
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table=source_config.ch_table,
            source_pk="",
            source_field=term.source_column,
            source_text=term.source_text,
            target_language="en",
        )
        for term in dynamic_terms
    ]
    dynamic_enqueued = queue.enqueue_items(items)
    return ScanAndSeedResult(dynamic_enqueued=dynamic_enqueued, static_flushed=static_flushed)


def flush_once(params: FlushInput) -> int:
    import os

    from translator.queue import TranslationQueue

    source_config = get_source_config(params.source_slug)
    rows = TranslationQueue(params.queue_duckdb_path).completed_results_for_flush()
    if not rows:
        return 0
    client = clickhouse_client_from_env()
    try:
        return flush_translations(
            client,
            source_config,
            rows,
            provider="local-llm",
            model=os.environ.get("TRANSLATION_PROVIDER_LOCAL_MODEL", "local-llm"),
            version=params.version,
            run_id=params.queue_duckdb_path,
        )
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


@activity.defn
async def scan_and_seed_activity(params: ScanAndSeedInput) -> ScanAndSeedResult:
    return await asyncio.to_thread(scan_and_seed_once, params)


@activity.defn
async def flush_activity(params: FlushInput) -> int:
    return await asyncio.to_thread(flush_once, params)


def _queue_path(queue_dir: str, source_slug: str) -> str:
    return f"{queue_dir.rstrip('/')}/{source_slug}.duckdb"


@workflow.defn
class TranslateSourceWorkflow:
    @workflow.run
    async def run(self, params: TranslateSourceWorkflowInput) -> TranslateSourceWorkflowOutput:
        queue_path = _queue_path(params.queue_dir, params.source_slug)

        scan_result: ScanAndSeedResult = await workflow.execute_activity(
            scan_and_seed_activity,
            ScanAndSeedInput(source_slug=params.source_slug, queue_duckdb_path=queue_path),
            start_to_close_timeout=timedelta(seconds=params.scan_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )

        failure_count = 0
        while True:
            result = await workflow.execute_activity(
                process_translation_batch,
                ProcessTranslationBatchInput(
                    duckdb_path=queue_path,
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
            if result.status != "success":
                failure_count += 1
                if failure_count > params.max_batch_failures:
                    break

        version = int(workflow.now().timestamp())
        dynamic_flushed = await workflow.execute_activity(
            flush_activity,
            FlushInput(source_slug=params.source_slug, queue_duckdb_path=queue_path, version=version),
            start_to_close_timeout=timedelta(seconds=params.flush_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )

        summary = await workflow.execute_activity(
            summarize_translation_queue,
            queue_path,
            start_to_close_timeout=timedelta(seconds=params.summarize_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
        )
        return TranslateSourceWorkflowOutput(
            enqueued_items=scan_result.dynamic_enqueued,
            completed_items=summary["completed_items"],
            failed_retryable_items=summary["failed_retryable_items"],
            flushed_rows=dynamic_flushed + scan_result.static_flushed,
        )
