from __future__ import annotations

import hashlib

import duckdb

from translations.queue import TranslationQueue, TranslationQueueItem
from translations.types import SmokeTranslationResult

TEST_MODEL = "test-model"


def test_queue_enqueues_and_claims_2000_items_in_50_item_batch(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()

    queue.enqueue_items(_items(2000))
    claimed = queue.claim_batch(limit=50, worker_id="worker-a")

    assert len(claimed) == 50
    assert {item.source_text for item in claimed}.issubset(
        {f"Allmennaksjeselskap {index}" for index in range(2000)}
    )

    summary = queue.summary()
    assert summary.total_items == 2000
    assert summary.location_items == 2000
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
        model=TEST_MODEL,
        duration_seconds=1.25,
    )

    summary = queue.summary()
    assert summary.total_items == 60
    assert summary.location_items == 60
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

    assert len(second_claim) == 10
    assert {item.item_id for item in second_claim}.isdisjoint(
        {item.item_id for item in first_claim}
    )

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
        model=TEST_MODEL,
        duration_seconds=0.1,
    )

    queue.enqueue_items(_items(1))

    assert queue.summary().completed_items == 1
    assert queue.summary().pending_items == 0


def test_queue_maps_duplicate_source_text_to_one_translation_item(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()
    queue.enqueue_items(
        [
            TranslationQueueItem(
                source_duckdb_path="/tmp/source.duckdb",
                source_table="synthetic_companies",
                source_pk="org-0001",
                source_field="description_original",
                source_text="Bygging av boliger",
                target_language="en",
            ),
            TranslationQueueItem(
                source_duckdb_path="/tmp/source.duckdb",
                source_table="synthetic_companies",
                source_pk="org-0002",
                source_field="description_original",
                source_text="Bygging av boliger",
                target_language="en",
            ),
        ]
    )

    summary = queue.summary()
    assert summary.total_items == 1
    assert summary.location_items == 2

    first_claim = queue.claim_batch(limit=50, worker_id="worker-a")
    assert len(first_claim) == 1
    assert first_claim[0].source_text == "Bygging av boliger"

    queue.complete_batch(
        first_claim,
        [SmokeTranslationResult(item_id=first_claim[0].item_id, translated_text="Construction of homes")],
        provider="fake",
        model=TEST_MODEL,
        duration_seconds=0.1,
    )

    second_claim = queue.claim_batch(limit=50, worker_id="worker-a")

    assert second_claim == []
    assert queue.summary().completed_items == 1
    assert queue.result_count() == 1
    assert [result.translated_text for result in queue.completed_results()] == [
        "Construction of homes",
        "Construction of homes",
    ]


def test_queue_does_not_create_model_specific_duplicate_translation_items(tmp_path) -> None:
    queue = TranslationQueue(tmp_path / "translations.duckdb")
    queue.initialize()
    item = TranslationQueueItem(
        source_duckdb_path="/tmp/source.duckdb",
        source_table="synthetic_companies",
        source_pk="org-0001",
        source_field="description_original",
        source_text="Bygging av boliger",
        target_language="en",
    )
    queue.enqueue_items([item])
    first_claim = queue.claim_batch(limit=50, worker_id="worker-a")
    queue.complete_batch(
        first_claim,
        [SmokeTranslationResult(item_id=first_claim[0].item_id, translated_text="Construction of homes")],
        provider="fake",
        model="model-a",
        duration_seconds=0.1,
    )
    queue.enqueue_items(
        [
            TranslationQueueItem(
                source_duckdb_path="/tmp/source.duckdb",
                source_table="synthetic_companies",
                source_pk="org-0002",
                source_field="description_original",
                source_text="Bygging av boliger",
                target_language="en",
            )
        ]
    )

    second_claim = queue.claim_batch(limit=50, worker_id="worker-b")

    assert second_claim == []
    assert queue.summary().total_items == 1
    assert queue.summary().location_items == 2


def test_queue_initialization_migrates_legacy_location_items(tmp_path) -> None:
    duckdb_path = tmp_path / "legacy_translations.duckdb"
    source_text = "Bygging av boliger"
    source_text_hash = hashlib.sha256(source_text.encode("utf-8")).hexdigest()
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(
            """
            create table translation_items (
                item_id text primary key,
                source_duckdb_path text not null,
                source_table text not null,
                source_pk text not null,
                source_field text not null,
                source_text text not null,
                source_text_hash text not null,
                target_language text not null,
                status text not null,
                attempt_count integer not null,
                leased_by text,
                leased_at timestamp,
                batch_id text,
                last_error_category text,
                last_error_message text,
                created_at timestamp not null,
                updated_at timestamp not null
            )
            """
        )
        connection.execute(
            """
            create table translation_results (
                item_id text primary key,
                translated_text text not null,
                provider text not null,
                completed_at timestamp not null
            )
            """
        )
        connection.execute(
            """
            create table translation_cache (
                model text not null,
                source_text_hash text not null,
                source_text text not null,
                translated_text text not null,
                completed_at timestamp not null,
                primary key (model, source_text_hash)
            )
            """
        )
        connection.execute(
            """
            create table translation_batch_attempts (
                batch_id text primary key,
                worker_id text not null,
                item_count integer not null,
                status text not null,
                started_at timestamp not null,
                finished_at timestamp not null,
                duration_seconds double not null,
                error_category text,
                error_message text
            )
            """
        )
        connection.executemany(
            """
            insert into translation_items values
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, null, current_timestamp, current_timestamp)
            """,
            [
                (
                    "legacy-location-1",
                    "/tmp/source.duckdb",
                    "synthetic_companies",
                    "org-0001",
                    "description_original",
                    source_text,
                    source_text_hash,
                    "en",
                    "completed",
                    0,
                ),
                (
                    "legacy-location-2",
                    "/tmp/source.duckdb",
                    "synthetic_companies",
                    "org-0002",
                    "description_original",
                    source_text,
                    source_text_hash,
                    "en",
                    "pending",
                    0,
                ),
            ],
        )
        connection.execute(
            """
            insert into translation_results values
            ('legacy-location-1', 'Construction of homes', 'fake', current_timestamp)
            """
        )
        connection.execute(
            """
            insert into translation_cache values
            ('test-model', ?, ?, 'Construction of homes', current_timestamp)
            """,
            [source_text_hash, source_text],
        )

    queue = TranslationQueue(duckdb_path)
    queue.initialize()

    summary = queue.summary()
    assert summary.total_items == 1
    assert summary.location_items == 2
    assert summary.completed_items == 1
    assert queue.result_count() == 1
    assert [result.translated_text for result in queue.completed_results()] == [
        "Construction of homes",
        "Construction of homes",
    ]
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        assert "translation_cache" not in {
            row[0] for row in connection.execute("show tables").fetchall()
        }


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
