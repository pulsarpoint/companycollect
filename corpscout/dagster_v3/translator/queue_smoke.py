from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import time
from typing import Protocol
from uuid import uuid4

from translator.provider_smoke import (
    _categorize_exception,
    _load_env_file,
    _parse_extra_body,
)
from translator.queue import (
    ClaimedTranslationItem,
    TranslationQueue,
    TranslationQueueItem,
)
from translator.smoke import (
    DEFAULT_EXTRA_BODY,
    DEFAULT_MAX_TOKENS,
    LocalOpenAICompatibleTranslationProvider,
)
from translator.types import SmokeTranslationInput, SmokeTranslationResult


DEFAULT_BATCH_SIZE = 50
DEFAULT_ITEM_COUNT = 2000


class TranslationProvider(Protocol):
    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]: ...


@dataclass(frozen=True)
class TranslationQueueSmokeMetrics:
    total_items: int
    completed_items: int
    pending_items: int
    leased_items: int
    failed_retryable_items: int
    result_items: int
    successful_batches: int
    failed_batches: int
    provider_success_count: int
    provider_failure_count: int
    total_duration_seconds: float
    average_batch_duration_seconds: float
    items_per_second: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def generate_synthetic_translation_items(
    count: int,
    *,
    source_duckdb_path: str,
) -> list[TranslationQueueItem]:
    samples = [
        "Allmennaksjeselskap",
        "Utvinning av raolje",
        "Utvinning av naturgass",
        "Produksjon av raffinerte petroleumsprodukter",
        "Selv, eller gjennom andre selskaper aa utvikle energi",
        "Handel med elektrisitet",
        "Konsulentvirksomhet tilknyttet informasjonsteknologi",
        "Bygging av boliger",
    ]
    return [
        TranslationQueueItem(
            source_duckdb_path=source_duckdb_path,
            source_table="synthetic_translation_items",
            source_pk=f"synthetic-{index:06d}",
            source_field="description_original",
            source_text=f"{samples[index % len(samples)]} {index}",
            target_language="en",
        )
        for index in range(count)
    ]


def run_translation_queue_smoke(
    *,
    duckdb_path: str | Path,
    item_count: int,
    batch_size: int,
    timeout_seconds: int,
    provider: TranslationProvider,
    worker_id: str,
    max_batch_failures: int,
) -> TranslationQueueSmokeMetrics:
    queue = TranslationQueue(duckdb_path)
    queue.initialize()
    queue.enqueue_items(
        generate_synthetic_translation_items(
            item_count,
            source_duckdb_path=str(duckdb_path),
        )
    )

    provider_success_count = 0
    provider_failure_count = 0
    batch_durations: list[float] = []
    started_at = time.perf_counter()
    while True:
        claimed = queue.claim_batch(limit=batch_size, worker_id=worker_id)
        if not claimed:
            break

        batch_started_at = time.perf_counter()
        provider_inputs = _provider_inputs(claimed)
        queue_id_by_provider_id = {
            provider_input.item_id: claimed_item.item_id
            for provider_input, claimed_item in zip(provider_inputs, claimed, strict=True)
        }
        try:
            translations = provider.translate(
                provider_inputs,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            duration_seconds = _elapsed_seconds(batch_started_at)
            provider_failure_count += 1
            batch_durations.append(duration_seconds)
            print(
                json.dumps(
                    {
                        "event": "translation_batch_failed",
                        "completed_items": queue.summary().completed_items,
                        "duration_seconds": duration_seconds,
                        "error_category": _categorize_exception(exc),
                        "error_message": str(exc),
                        "failed_batches": provider_failure_count,
                        "item_count": len(claimed),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            queue.fail_batch(
                claimed,
                error_category=_categorize_exception(exc),
                error_message=str(exc),
                duration_seconds=duration_seconds,
            )
            if max_batch_failures > 0 and provider_failure_count > max_batch_failures:
                break
            continue

        duration_seconds = _elapsed_seconds(batch_started_at)
        provider_success_count += 1
        batch_durations.append(duration_seconds)
        mapped_translations = _map_provider_results_to_queue_ids(
            translations,
            queue_id_by_provider_id=queue_id_by_provider_id,
        )
        queue.complete_batch(
            claimed,
            mapped_translations,
            provider=type(provider).__name__,
            model=str(getattr(provider, "model", type(provider).__name__)),
            duration_seconds=duration_seconds,
        )
        summary = queue.summary()
        print(
            json.dumps(
                {
                    "event": "translation_batch_completed",
                    "completed_items": summary.completed_items,
                    "duration_seconds": duration_seconds,
                    "item_count": len(claimed),
                    "pending_items": summary.pending_items,
                    "successful_batches": provider_success_count,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    total_duration_seconds = _elapsed_seconds(started_at)
    summary = queue.summary()
    return TranslationQueueSmokeMetrics(
        total_items=summary.total_items,
        completed_items=summary.completed_items,
        pending_items=summary.pending_items,
        leased_items=summary.leased_items,
        failed_retryable_items=summary.failed_retryable_items,
        result_items=summary.result_items,
        successful_batches=summary.successful_batches,
        failed_batches=summary.failed_batches,
        provider_success_count=provider_success_count,
        provider_failure_count=provider_failure_count,
        total_duration_seconds=total_duration_seconds,
        average_batch_duration_seconds=_average(batch_durations),
        items_per_second=round(summary.completed_items / total_duration_seconds, 3)
        if total_duration_seconds > 0
        else 0.0,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a DuckDB translation queue smoke loop.")
    parser.add_argument("--duckdb-path", required=True, help="DuckDB file for queue state.")
    parser.add_argument("--item-count", default=DEFAULT_ITEM_COUNT, type=int)
    parser.add_argument("--batch-size", default=DEFAULT_BATCH_SIZE, type=int)
    parser.add_argument("--timeout-seconds", default=120, type=int)
    parser.add_argument("--max-batch-failures", default=0, type=int)
    parser.add_argument("--worker-id", default=f"queue-smoke-{uuid4()}")
    parser.add_argument(
        "--max-tokens",
        default=int(os.getenv("TRANSLATION_PROVIDER_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))),
        type=int,
    )
    parser.add_argument(
        "--extra-body-json",
        default=os.getenv(
            "TRANSLATION_PROVIDER_EXTRA_BODY_JSON",
            json.dumps(DEFAULT_EXTRA_BODY, separators=(",", ":")),
        ),
    )
    parser.add_argument("--env-file", default=".env")
    args = parser.parse_args(argv)

    _load_env_file(Path(args.env_file))
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
        model=os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"],
        api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
        max_tokens=args.max_tokens,
        extra_body=_parse_extra_body(args.extra_body_json),
    )
    metrics = run_translation_queue_smoke(
        duckdb_path=args.duckdb_path,
        item_count=args.item_count,
        batch_size=args.batch_size,
        timeout_seconds=args.timeout_seconds,
        provider=provider,
        worker_id=args.worker_id,
        max_batch_failures=args.max_batch_failures,
    )
    print(json.dumps(metrics.to_dict(), indent=2, sort_keys=True))
    return 0 if metrics.completed_items == metrics.total_items else 1


def _provider_inputs(items: list[ClaimedTranslationItem]) -> list[SmokeTranslationInput]:
    return [
        SmokeTranslationInput(
            item_id=f"batch-item-{index:03d}",
            source_text=item.source_text,
        )
        for index, item in enumerate(items)
    ]


def _map_provider_results_to_queue_ids(
    translations: list[SmokeTranslationResult],
    *,
    queue_id_by_provider_id: dict[str, str],
) -> list[SmokeTranslationResult]:
    return [
        SmokeTranslationResult(
            item_id=queue_id_by_provider_id[translation.item_id],
            translated_text=translation.translated_text,
        )
        for translation in translations
    ]


def _elapsed_seconds(started_at: float) -> float:
    return round(time.perf_counter() - started_at, 3)


def _average(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)


if __name__ == "__main__":
    raise SystemExit(main())
