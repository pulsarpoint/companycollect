"""Temporal WorkflowEnvironment tests for BuildQueueWorkflow + TranslateWorkflow."""
from __future__ import annotations

import asyncio
import uuid

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
        max_batch_failures=5,
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-bq",
                workflows=[wf.BuildQueueWorkflow],
                activities=[wf.build_queue_activity, _fake_start],
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
        max_batch_failures=5,
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
            ), Worker(
                env.client,
                task_queue=wf.LLM_TASK_QUEUE,
                activities=[wf.translate_loop_activity],
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


def test_translate_loop_once_stops_at_max_batch_failures(tmp_path, monkeypatch):
    """translate_loop_once must stop after exactly max_batch_failures failed batches.

    With max_batch_failures=2 and a provider that always fails, the loop must
    break after exactly 2 failures (not 3, which the old failure_count >
    max_batch_failures predicate would have tolerated).
    """
    import temporalio.activity
    import translator.llm_batch
    import translator.provider as translator_provider

    # Seed 5 items (batch_size=1 → up to 5 separate claim_batch calls possible).
    queue_path = _seed_queue(tmp_path, ["A", "B", "C", "D", "E"])

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
        raise RuntimeError("LLM unavailable")

    monkeypatch.setattr(translator.llm_batch, "translate_batch", _always_fail)
    # Heartbeat is a no-op outside a real Temporal activity context.
    monkeypatch.setattr(temporalio.activity, "heartbeat", lambda *a, **kw: None)

    result = wf.translate_loop_once(
        wf.TranslateLoopActivityInput(
            queue_duckdb_path=queue_path,
            batch_size=1,
            max_tokens=64,
            extra_body_json="{}",
            max_batch_failures=2,
        )
    )

    assert result.failed_batches == 2, (
        f"expected exactly 2 failed batches, got {result.failed_batches}"
    )
    assert result.completed_items == 0
    assert result.successful_batches == 0


def test_translate_workflow_tolerates_max_batch_failures(tmp_path, monkeypatch):
    """TranslateWorkflow must stop after max_batch_failures and still dump."""
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
        max_batch_failures=5,
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-tr2",
                workflows=[wf.TranslateWorkflow],
                activities=[wf.dump_activity, wf.summarize_queue_activity],
            ), Worker(
                env.client,
                task_queue=wf.LLM_TASK_QUEUE,
                activities=[wf.translate_loop_activity],
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
