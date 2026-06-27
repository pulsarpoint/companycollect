from translator.queue import FlushTranslationRow, TranslationQueue, TranslationQueueItem
from translator.types import TranslationResult


def _item(source_column: str, text: str) -> TranslationQueueItem:
    return TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.companies",
        source_pk="",
        source_field=source_column,
        source_text=text,
        target_language="en",
    )


def test_completed_results_for_flush_returns_source_column_text_translation(tmp_path):
    queue = TranslationQueue(tmp_path / "q.duckdb")
    queue.initialize()
    queue.enqueue_items([_item("activity_text_original", "Holdingselskap")])

    claimed = queue.claim_batch(limit=10, worker_id="w1")
    queue.complete_batch(
        claimed,
        [TranslationResult(item_id=claimed[0].item_id, translated_text="Holding company")],
        provider="prov",
        model="model",
        duration_seconds=0.1,
    )

    result = queue.completed_results_for_flush()
    assert result == [
        FlushTranslationRow(
            source_column="activity_text_original",
            source_text="Holdingselskap",
            translated_text="Holding company",
        )
    ]
    assert result[0].source_column == "activity_text_original"


def test_completed_results_for_flush_excludes_pending(tmp_path):
    queue = TranslationQueue(tmp_path / "q.duckdb")
    queue.initialize()
    queue.enqueue_items([_item("activity_text_original", "Bygg og anlegg")])
    assert queue.completed_results_for_flush() == []
