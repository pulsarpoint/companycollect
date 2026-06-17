from __future__ import annotations

from translations.queue import TranslationQueue, TranslationQueueItem
from translations.types import SmokeTranslationResult


def test_queue_enqueues_and_claims_2000_items_in_50_item_batch(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()

    queue.enqueue_items(_items(2000))
    claimed = queue.claim_batch(limit=50, worker_id="worker-a")

    assert len(claimed) == 50
    assert claimed[0].source_text == "Allmennaksjeselskap 0"
    assert claimed[-1].source_text == "Allmennaksjeselskap 49"

    summary = queue.summary()
    assert summary.total_items == 2000
    assert summary.pending_items == 1950
    assert summary.leased_items == 50
    assert summary.completed_items == 0


def test_queue_complete_batch_writes_results_and_removes_leases(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()
    queue.enqueue_items(_items(60))
    claimed = queue.claim_batch(limit=50, worker_id="worker-a")

    queue.complete_batch(
        claimed,
        [
            SmokeTranslationResult(
                item_id=item.item_id,
                translated_text=f"Translated {item.item_id}",
            )
            for item in claimed
        ],
        provider="fake",
        duration_seconds=1.25,
    )

    summary = queue.summary()
    assert summary.total_items == 60
    assert summary.pending_items == 10
    assert summary.leased_items == 0
    assert summary.completed_items == 50
    assert summary.failed_retryable_items == 0
    assert summary.batch_attempts == 1
    assert summary.successful_batches == 1

    assert queue.result_count() == 50


def test_queue_claims_pending_items_before_retryable_failures(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()
    queue.enqueue_items(_items(60))
    first_claim = queue.claim_batch(limit=50, worker_id="worker-a")

    queue.fail_batch(
        first_claim,
        error_category="invalid_json",
        error_message="bad response",
        duration_seconds=0.75,
    )
    second_claim = queue.claim_batch(limit=50, worker_id="worker-b")

    assert [item.source_text for item in second_claim] == [
        f"Allmennaksjeselskap {index}" for index in range(50, 60)
    ]

    retry_claim = queue.claim_batch(limit=50, worker_id="worker-c")
    assert [item.item_id for item in retry_claim] == [item.item_id for item in first_claim]

    summary = queue.summary()
    assert summary.pending_items == 0
    assert summary.leased_items == 60
    assert summary.failed_retryable_items == 0
    assert summary.completed_items == 0
    assert summary.batch_attempts == 1
    assert summary.failed_batches == 1
    assert all(item.attempt_count == 1 for item in retry_claim)


def test_queue_does_not_reenqueue_completed_item(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()
    queue.enqueue_items(_items(1))
    claimed = queue.claim_batch(limit=1, worker_id="worker-a")
    queue.complete_batch(
        claimed,
        [SmokeTranslationResult(item_id=claimed[0].item_id, translated_text="Done")],
        provider="fake",
        duration_seconds=0.1,
    )

    queue.enqueue_items(_items(1))

    assert queue.summary().completed_items == 1
    assert queue.summary().pending_items == 0


def _items(count: int) -> list[TranslationQueueItem]:
    return [
        TranslationQueueItem(
            source_duckdb_path="/tmp/source.duckdb",
            source_table="synthetic_companies",
            source_pk=f"org-{index:04d}",
            source_field="description_original",
            source_text=f"Allmennaksjeselskap {index}",
            target_language="en",
        )
        for index in range(count)
    ]
