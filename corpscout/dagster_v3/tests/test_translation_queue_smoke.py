from __future__ import annotations

from translations.queue_smoke import (
    generate_synthetic_translation_items,
    run_translation_queue_smoke,
)
from translations.types import SmokeTranslationInput, SmokeTranslationResult


class _SuccessfulProvider:
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []
        self.seen_item_ids: list[list[str]] = []

    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        self.batch_sizes.append(len(items))
        self.seen_item_ids.append([item.item_id for item in items])
        return [
            SmokeTranslationResult(
                item_id=item.item_id,
                translated_text=f"English {item.source_text}",
            )
            for item in items
        ]


class _FailOnceProvider(_SuccessfulProvider):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        if not self.failed:
            self.failed = True
            raise ValueError("invalid json")
        return super().translate(items, timeout_seconds=timeout_seconds)


def test_generate_synthetic_translation_items_returns_requested_count() -> None:
    items = generate_synthetic_translation_items(2000, source_duckdb_path="/tmp/smoke.duckdb")

    assert len(items) == 2000
    assert items[0].source_pk == "synthetic-000000"
    assert items[-1].source_pk == "synthetic-001999"
    assert all(item.target_language == "en" for item in items)


def test_run_translation_queue_smoke_processes_items_in_50_item_batches(tmp_path) -> None:
    provider = _SuccessfulProvider()

    metrics = run_translation_queue_smoke(
        duckdb_path=tmp_path / "queue.duckdb",
        item_count=120,
        batch_size=50,
        timeout_seconds=30,
        provider=provider,
        worker_id="worker-a",
        max_batch_failures=0,
    )

    assert provider.batch_sizes == [50, 50, 20]
    assert metrics.total_items == 120
    assert metrics.completed_items == 120
    assert metrics.failed_retryable_items == 0
    assert metrics.successful_batches == 3
    assert metrics.failed_batches == 0
    assert metrics.items_per_second > 0


def test_run_translation_queue_smoke_retries_failed_batch(tmp_path) -> None:
    provider = _FailOnceProvider()

    metrics = run_translation_queue_smoke(
        duckdb_path=tmp_path / "queue.duckdb",
        item_count=60,
        batch_size=50,
        timeout_seconds=30,
        provider=provider,
        worker_id="worker-a",
        max_batch_failures=1,
    )

    assert provider.batch_sizes == [10, 50]
    assert metrics.total_items == 60
    assert metrics.completed_items == 60
    assert metrics.successful_batches == 2
    assert metrics.failed_batches == 1
    assert metrics.provider_failure_count == 1


def test_run_translation_queue_smoke_zero_max_batch_failures_continues_after_failure(
    tmp_path,
) -> None:
    provider = _FailOnceProvider()

    metrics = run_translation_queue_smoke(
        duckdb_path=tmp_path / "queue.duckdb",
        item_count=60,
        batch_size=50,
        timeout_seconds=30,
        provider=provider,
        worker_id="worker-a",
        max_batch_failures=0,
    )

    assert metrics.total_items == 60
    assert metrics.completed_items == 60
    assert metrics.failed_batches == 1
    assert metrics.provider_failure_count == 1
    assert metrics.successful_batches == 2


def test_run_translation_queue_smoke_sends_short_batch_local_ids_to_provider(tmp_path) -> None:
    provider = _SuccessfulProvider()

    metrics = run_translation_queue_smoke(
        duckdb_path=tmp_path / "queue.duckdb",
        item_count=3,
        batch_size=3,
        timeout_seconds=30,
        provider=provider,
        worker_id="worker-a",
        max_batch_failures=0,
    )

    assert provider.seen_item_ids == [["batch-item-000", "batch-item-001", "batch-item-002"]]
    assert metrics.completed_items == 3
    assert metrics.result_items == 3
