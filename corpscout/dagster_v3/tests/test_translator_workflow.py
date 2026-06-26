import asyncio
import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from translator import activities as acts
from translator import workflow as wf


def test_translate_source_workflow_scans_translates_flushes(tmp_path, monkeypatch):
    monkeypatch.setattr(wf, "clickhouse_client_from_env", lambda: object())
    monkeypatch.setattr(
        wf,
        "scan_untranslated_terms",
        lambda client, cfg: [("company_description", "Holdingselskap"), ("activity_text", "Bygg")],
    )

    flushed = {}

    def fake_flush(client, cfg, rows, *, provider, model, version, run_id):
        flushed["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(wf, "flush_translations", fake_flush)

    def fake_process_once(params, *, provider=None):
        from translator.queue import TranslationQueue
        from translator.types import SmokeTranslationResult

        q = TranslationQueue(params.duckdb_path)
        q.initialize()
        claimed = q.claim_batch(limit=params.batch_size, worker_id=params.worker_id)
        if not claimed:
            return acts.ProcessTranslationBatchResult(status="empty", item_count=0, duration_seconds=0.0)
        q.complete_batch(
            claimed,
            [SmokeTranslationResult(item_id=c.item_id, translated_text=c.source_text.upper()) for c in claimed],
            provider="fake", model="fake", duration_seconds=0.0,
        )
        return acts.ProcessTranslationBatchResult(status="success", item_count=len(claimed), duration_seconds=0.0)

    monkeypatch.setattr(acts, "process_translation_batch_once", fake_process_once)

    params = wf.TranslateSourceWorkflowInput(
        source_slug="norway_brreg", queue_dir=str(tmp_path), batch_size=10, timeout_seconds=5,
        max_batch_failures=0, worker_id="test-worker", max_tokens=64, extra_body_json="",
        initialize_timeout_seconds=10, batch_timeout_buffer_seconds=5, summarize_timeout_seconds=10,
        activity_maximum_attempts=1, lease_timeout_seconds=60, scan_timeout_seconds=10, flush_timeout_seconds=10,
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client, task_queue="test-translator",
                workflows=[wf.TranslateSourceWorkflow],
                activities=[wf.scan_and_seed_activity, wf.flush_activity,
                            acts.process_translation_batch, acts.summarize_translation_queue],
            ):
                return await env.client.execute_workflow(
                    wf.TranslateSourceWorkflow.run, params,
                    id=f"test-{uuid.uuid4()}", task_queue="test-translator",
                )

    result = asyncio.run(_run())

    assert result.enqueued_items == 2
    assert result.completed_items == 2
    assert result.flushed_rows == 2
    assert sorted(r.translated_text for r in flushed["rows"]) == ["BYGG", "HOLDINGSELSKAP"]
