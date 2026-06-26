from translator.queue import FlushTranslationRow, TranslationQueue, TranslationQueueItem
from translator.types import SmokeTranslationResult


def _item(field: str, text: str) -> TranslationQueueItem:
    return TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.companies",
        source_pk="",
        source_field=field,
        source_text=text,
        target_language="en",
    )


def test_completed_results_for_flush_returns_field_text_translation(tmp_path):
    queue = TranslationQueue(tmp_path / "q.duckdb")
    queue.initialize()
    queue.enqueue_items([_item("company_description", "Holdingselskap")])

    claimed = queue.claim_batch(limit=10, worker_id="w1")
    queue.complete_batch(
        claimed,
        [SmokeTranslationResult(item_id=claimed[0].item_id, translated_text="Holding company")],
        provider="prov",
        model="model",
        duration_seconds=0.1,
    )

    assert queue.completed_results_for_flush() == [
        FlushTranslationRow("company_description", "Holdingselskap", "Holding company")
    ]


def test_completed_results_for_flush_excludes_pending(tmp_path):
    queue = TranslationQueue(tmp_path / "q.duckdb")
    queue.initialize()
    queue.enqueue_items([_item("activity_text", "Bygg og anlegg")])
    assert queue.completed_results_for_flush() == []
