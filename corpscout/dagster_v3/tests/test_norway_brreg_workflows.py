"""Temporal WorkflowEnvironment tests for BuildQueueWorkflow + TranslateWorkflow."""
from __future__ import annotations

import asyncio
import concurrent.futures
import uuid

import pyarrow as pa
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from translator.norway_brreg.seed import SeedResult
from translator.norway_brreg import workflows as wf
from translator.queue import TranslationQueue, TranslationQueueItem
from translator.types import TranslationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_queue(tmp_path, texts: list[str]) -> str:
    """Pre-seed a DuckDB queue with pending items and return path as str."""
    path = tmp_path / "q.duckdb"
    q = TranslationQueue(path)
    q.initialize()
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table="corpscout.no_companies",
            source_pk="",
            source_field="activity_text_original",
            source_text=text,
            target_language="en",
        )
        for text in texts
    ]
    q.enqueue_items(items)
    return str(path)


# ---------------------------------------------------------------------------
# BuildQueueWorkflow tests
# ---------------------------------------------------------------------------


def test_build_queue_workflow_seeds_then_starts_translate(tmp_path, monkeypatch):
    """BuildQueueWorkflow must call seed, then start TranslateWorkflow (USE_EXISTING)."""
    seed_calls = []
    start_calls = []

    def fake_build_queue_once(params: wf.BuildQueueActivityInput) -> SeedResult:
        seed_calls.append(params)
        return SeedResult(dynamic_enqueued=7, static_flushed=1)

    monkeypatch.setattr(wf, "build_queue_once", fake_build_queue_once)

    # Register a fake handoff activity (same name as the real one) so the
    # workflow's start_translate_workflow_activity call resolves to this stub
    # instead of connecting to a real Temporal server.
    @activity.defn(name="start_translate_workflow_activity")
    async def _fake_start(params: wf.StartTranslateWorkflowInput) -> str:
        start_calls.append(params)
        return "fake-translate-run-id"

    params = wf.BuildQueueWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=str(tmp_path / "q.duckdb"),
        translate_workflow_id="translate-norway_brreg",
        translate_task_queue="translation-build",
        batch_size=50,
        max_tokens=8192,
        extra_body_json="{}",
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-bq",
                workflows=[wf.BuildQueueWorkflow],
                activities=[wf.build_queue_activity, _fake_start],
                # Sync activities require an executor (heartbeat uses
                # run_coroutine_threadsafe — only available for sync+executor).
                activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
            ):
                return await env.client.execute_workflow(
                    wf.BuildQueueWorkflow.run,
                    params,
                    id=f"test-{uuid.uuid4()}",
                    task_queue="test-bq",
                )

    result = asyncio.run(_run())

    assert result.dynamic_enqueued == 7
    assert result.static_flushed == 1
    assert len(seed_calls) == 1
    assert len(start_calls) == 1
    # Handoff must target the correct workflow id.
    assert start_calls[0].workflow_id == "translate-norway_brreg"
    assert start_calls[0].source_slug == "norway_brreg"


# ---------------------------------------------------------------------------
# TranslateWorkflow tests
# ---------------------------------------------------------------------------


def test_translate_workflow_drains_queue_and_dumps(tmp_path, monkeypatch):
    """TranslateWorkflow must drain queue items then dump to ClickHouse."""
    queue_path = _seed_queue(tmp_path, ["Holdingselskap", "Bygg"])
    dump_calls = []

    def fake_translate_loop_once(params: wf.TranslateLoopActivityInput) -> wf.TranslateLoopResult:
        # Directly drain the real DuckDB queue using the fake provider.
        from translator.queue import TranslationQueue as TQ
        q = TQ(params.queue_duckdb_path)
        q.initialize()
        total_completed = 0
        while True:
            claimed = q.claim_batch(limit=params.batch_size, worker_id="test")
            if not claimed:
                break
            q.complete_batch(
                claimed,
                [TranslationResult(item_id=c.item_id, translated_text=c.source_text.upper()) for c in claimed],
                provider="fake",
                model="fake",
                duration_seconds=0.0,
            )
            total_completed += len(claimed)
        return wf.TranslateLoopResult(
            completed_items=total_completed, failed_batches=0, successful_batches=1
        )

    monkeypatch.setattr(wf, "translate_loop_once", fake_translate_loop_once)

    def fake_dump_once(params: wf.DumpActivityInput) -> int:
        dump_calls.append(params)
        return 2

    monkeypatch.setattr(wf, "dump_once", fake_dump_once)

    translate_params = wf.TranslateWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=queue_path,
        batch_size=10,
        max_tokens=64,
        extra_body_json="{}",
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            # Build worker (workflow + dump + summarize on the workflow's own queue)
            # AND a separate LLM-queue worker for translate_loop_activity — this
            # proves the loop is routed to LLM_TASK_QUEUE (only that worker can
            # run it; if routing were wrong the workflow would never complete).
            async with Worker(
                env.client,
                task_queue="test-tr",
                workflows=[wf.TranslateWorkflow],
                activities=[wf.dump_activity, wf.summarize_queue_activity],
                activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
            ), Worker(
                env.client,
                task_queue=wf.LLM_TASK_QUEUE,
                activities=[wf.translate_loop_activity],
                activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
            ):
                return await env.client.execute_workflow(
                    wf.TranslateWorkflow.run,
                    translate_params,
                    id=f"test-{uuid.uuid4()}",
                    task_queue="test-tr",
                )

    result = asyncio.run(_run())

    assert result.completed_items == 2
    assert result.flushed_rows == 2
    assert len(dump_calls) == 1
    assert dump_calls[0].queue_duckdb_path == queue_path


def test_translate_loop_once_drains_via_attempt_cap(tmp_path, monkeypatch):
    """translate_loop_once must drain via the per-item attempt cap (not max_batch_failures).

    With provider_error (retryable) and DEFAULT_MAX_ATTEMPTS=3, each item gets 3
    attempts then goes terminal. The loop ends only when claim_batch returns empty.
    With 2 items and batch_size=2, there should be exactly 3 failure batches total
    (one per attempt round) before the loop drains.
    """
    import temporalio.activity
    import translator.llm_batch
    import translator.provider as translator_provider
    from translator.queue import DEFAULT_MAX_ATTEMPTS

    # Seed 2 items (batch_size=2 → each claim gets both items at once).
    queue_path = _seed_queue(tmp_path, ["A", "B"])

    class _FakeProvider:
        def close(self) -> None:
            pass

    monkeypatch.setenv("TRANSLATION_PROVIDER_LOCAL_MODEL", "fake-model")
    monkeypatch.setenv("TRANSLATION_PROVIDER_LOCAL_BASE_URL", "http://fake-url")
    monkeypatch.setattr(
        translator_provider,
        "LocalOpenAICompatibleTranslationProvider",
        lambda **kw: _FakeProvider(),
    )

    def _always_fail(*args, **kwargs):
        # RuntimeError maps to provider_error (retryable), so items retry up to cap.
        raise RuntimeError("LLM unavailable")

    monkeypatch.setattr(translator.llm_batch, "translate_batch", _always_fail)
    monkeypatch.setattr(temporalio.activity, "heartbeat", lambda *a, **kw: None)

    result = wf.translate_loop_once(
        wf.TranslateLoopActivityInput(
            queue_duckdb_path=queue_path,
            batch_size=2,
            max_tokens=64,
            extra_body_json="{}",
        )
    )

    # With DEFAULT_MAX_ATTEMPTS=3 and 2 items per batch, the loop runs exactly
    # DEFAULT_MAX_ATTEMPTS times (once per attempt round), then claim returns empty.
    assert result.failed_batches == DEFAULT_MAX_ATTEMPTS, (
        f"expected exactly {DEFAULT_MAX_ATTEMPTS} failed batches (one per attempt), "
        f"got {result.failed_batches}"
    )
    assert result.completed_items == 0
    assert result.successful_batches == 0


def test_translate_workflow_completes_after_all_failures(tmp_path, monkeypatch):
    """TranslateWorkflow must complete (dump + summarize) even when the loop returns only failures."""
    queue_path = _seed_queue(tmp_path, ["A", "B"])
    dump_calls = []

    def always_fail_loop(params: wf.TranslateLoopActivityInput) -> wf.TranslateLoopResult:
        return wf.TranslateLoopResult(completed_items=0, failed_batches=6, successful_batches=0)

    monkeypatch.setattr(wf, "translate_loop_once", always_fail_loop)

    def fake_dump_once(params: wf.DumpActivityInput) -> int:
        dump_calls.append(params)
        return 0

    monkeypatch.setattr(wf, "dump_once", fake_dump_once)

    translate_params = wf.TranslateWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=queue_path,
        batch_size=10,
        max_tokens=64,
        extra_body_json="{}",
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-tr2",
                workflows=[wf.TranslateWorkflow],
                activities=[wf.dump_activity, wf.summarize_queue_activity],
                activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
            ), Worker(
                env.client,
                task_queue=wf.LLM_TASK_QUEUE,
                activities=[wf.translate_loop_activity],
                activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
            ):
                return await env.client.execute_workflow(
                    wf.TranslateWorkflow.run,
                    translate_params,
                    id=f"test-{uuid.uuid4()}",
                    task_queue="test-tr2",
                )

    result = asyncio.run(_run())
    # dump still called even on failure.
    assert len(dump_calls) == 1
    assert result.failed_batches == 6


# ---------------------------------------------------------------------------
# Integration test: real build_queue_activity via real Worker + fake CH client
#
# This test exercises the FULL activity path through a real Temporal worker
# (WorkflowEnvironment) to catch the 'no running event loop' regression where
# build_queue_once called activity.heartbeat() inside asyncio.to_thread().
# ---------------------------------------------------------------------------


class _FakeCHClient:
    """Minimal ClickHouse fake that returns pre-canned Arrow tables.

    Mirrors the fake in test_norway_brreg_seed.py but lives here to keep
    this file self-contained.
    """

    def __init__(self, arrow_per_column: dict[str, pa.Table]):
        self._data = arrow_per_column
        self.calls: list[str] = []

    def query_arrow(self, sql: str, *, parameters: dict | None = None) -> pa.Table:
        col = (parameters or {}).get("column", "")
        self.calls.append(col)
        return self._data.get(col, pa.table({"source_text": pa.array([], type=pa.string())}))

    def command(self, sql, parameters=None):
        pass

    def insert(self, table, data, column_names=None):
        pass

    def close(self):
        pass


def test_build_queue_activity_heartbeat_via_real_worker(tmp_path, monkeypatch):
    """build_queue_activity must complete when run through a real Temporal worker.

    This is the regression test for the 'no running event loop' crash.  The
    bug was that build_queue_activity was declared async and delegated to
    asyncio.to_thread.  When build_queue_once called activity.heartbeat() from
    that thread, temporalio's _heartbeat() called asyncio.create_task() which
    raises RuntimeError from a non-event-loop thread.

    The fix: declare the four blocking activities as plain sync defs and give
    both workers an activity_executor=ThreadPoolExecutor.  Temporalio then
    wraps the heartbeat with asyncio.run_coroutine_threadsafe before passing it
    to the worker thread — the correct thread-safe path.
    """
    queue_path = str(tmp_path / "q.duckdb")

    fake_ch = _FakeCHClient({
        "articles_purpose_original": pa.table(
            {"source_text": pa.array(["Holding", "Bygg"], type=pa.string())}
        ),
        "activity_text_original": pa.table(
            {"source_text": pa.array(["Energi"], type=pa.string())}
        ),
        "legal_form_description_original": pa.table({
            "source_text": pa.array(["Aksjeselskap"], type=pa.string()),
            "static_key": pa.array(["AS"], type=pa.string()),
        }),
    })

    import translator.clickhouse as _ch_mod
    monkeypatch.setattr(_ch_mod, "clickhouse_client_from_env", lambda: fake_ch)

    params = wf.BuildQueueWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=queue_path,
        translate_workflow_id="translate-norway_brreg-test",
        translate_task_queue="test-bq-integ-llm",
        batch_size=50,
        max_tokens=8192,
        extra_body_json="{}",
    )

    @activity.defn(name="start_translate_workflow_activity")
    async def _fake_start(p: wf.StartTranslateWorkflowInput) -> str:
        return "fake-translate-workflow-id"

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
            async with Worker(
                env.client,
                task_queue="test-bq-integ",
                workflows=[wf.BuildQueueWorkflow],
                activities=[wf.build_queue_activity, _fake_start],
                activity_executor=executor,
                max_concurrent_activities=8,
            ):
                return await env.client.execute_workflow(
                    wf.BuildQueueWorkflow.run,
                    params,
                    id=f"test-integ-{uuid.uuid4()}",
                    task_queue="test-bq-integ",
                )

    result = asyncio.run(_run())

    # The seed must have processed items from the fake CH client.
    assert result.dynamic_enqueued >= 0  # no crash = the fix works

    # The DuckDB queue must contain the seeded items (3 dynamic texts).
    q = TranslationQueue(queue_path)
    q.initialize()
    summary = q.summary()
    assert summary.total_items == 3, (
        f"expected 3 items in queue, got {summary.total_items}"
    )
