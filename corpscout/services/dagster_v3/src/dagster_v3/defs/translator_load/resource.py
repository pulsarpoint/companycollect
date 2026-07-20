"""Dagster resource for the shared translator service HTTP API."""

import time
from collections.abc import Sequence
from dataclasses import dataclass

import dagster as dg
import requests

MAX_ITEMS_PER_REQUEST = 10_000

type SourceTextHashRows = Sequence[tuple[str, int]]


@dataclass(frozen=True)
class TranslationEnqueueResult:
    received: int
    inserted: int
    workflow_start_warnings: tuple[str, ...]


@dataclass(frozen=True)
class TranslationQueueStats:
    input: int
    pending: int
    output: int
    failed: int

    def is_idle(self) -> bool:
        return self.input == 0 and self.pending == 0 and self.output == 0

    def as_metadata(self) -> dict[str, int]:
        return {
            "input": self.input,
            "pending": self.pending,
            "output": self.output,
            "failed": self.failed,
        }


class TranslationQueueFailedError(RuntimeError):
    def __init__(self, stats: TranslationQueueStats) -> None:
        self.stats = stats
        super().__init__(
            f"translator queue contains {stats.failed} failed translation items"
        )


class TranslationQueueTimeoutError(TimeoutError):
    def __init__(
        self,
        *,
        timeout_seconds: int,
        stats: TranslationQueueStats,
    ) -> None:
        self.stats = stats
        super().__init__(
            "translator queue did not finish within "
            f"{timeout_seconds} seconds "
            f"(input={stats.input}, pending={stats.pending}, "
            f"output={stats.output}, failed={stats.failed})"
        )


class TranslatorResource(dg.ConfigurableResource):
    """Client for enqueueing source text and inspecting the translation queue."""

    base_url: str
    enqueue_timeout_seconds: int = 60
    stats_timeout_seconds: int = 10
    completion_timeout_seconds: int = 7_200
    completion_poll_interval_seconds: float = 2.0

    def enqueue_translation_rows(
        self,
        *,
        source_table: str,
        source_column: str,
        source_lang: str,
        target_lang: str,
        source_language_name: str,
        target_language_name: str,
        rows: SourceTextHashRows,
    ) -> TranslationEnqueueResult:
        received = 0
        inserted = 0
        warnings: list[str] = []

        with requests.Session() as session:
            for start in range(0, len(rows), MAX_ITEMS_PER_REQUEST):
                chunk = rows[start : start + MAX_ITEMS_PER_REQUEST]
                response = session.post(
                    f"{self.base_url.rstrip('/')}/v1/queue/items",
                    json={
                        "source_lang": source_lang,
                        "target_lang": target_lang,
                        "source_language_name": source_language_name,
                        "target_language_name": target_language_name,
                        "items": [
                            {
                                "source_table": source_table,
                                "source_column": source_column,
                                "source_text": text,
                                "source_text_hash": str(text_hash),
                            }
                            for text, text_hash in chunk
                        ],
                    },
                    timeout=self.enqueue_timeout_seconds,
                )
                response.raise_for_status()
                response_body = response.json()
                received += int(response_body["received"])
                inserted += int(response_body["inserted"])
                warning = response_body.get("warning")
                if warning:
                    warnings.append(str(warning))

        return TranslationEnqueueResult(
            received=received,
            inserted=inserted,
            workflow_start_warnings=tuple(warnings),
        )

    def queue_stats(self) -> TranslationQueueStats:
        with requests.Session() as session:
            response = session.get(
                f"{self.base_url.rstrip('/')}/v1/queue/stats",
                timeout=self.stats_timeout_seconds,
            )
            response.raise_for_status()
            stats = response.json()

        return TranslationQueueStats(
            input=int(stats.get("input", 0)),
            pending=int(stats.get("pending", 0)),
            output=int(stats.get("output", 0)),
            failed=int(stats.get("failed", 0)),
        )

    def wait_for_queue_completion(
        self, *, baseline_failed: int = 0
    ) -> TranslationQueueStats:
        """Poll queue stats until idle.

        ``baseline_failed``: the queue's failed count BEFORE this run
        enqueued (failed items are permanent in the queue db and the stats
        are global across sources, so a leftover failure from another
        source must not fail this loader forever — only failures that
        appear DURING this wait are this run's problem).
        """
        if self.completion_timeout_seconds <= 0:
            raise ValueError("translator completion timeout must be positive")
        if self.completion_poll_interval_seconds <= 0:
            raise ValueError("translator completion poll interval must be positive")

        deadline = time.monotonic() + self.completion_timeout_seconds
        while True:
            stats = self.queue_stats()
            if stats.failed > baseline_failed:
                raise TranslationQueueFailedError(stats)
            # Terminal: nothing pending, everything flushed, and the only
            # rows left in input are the permanent failed residue (failed
            # items are never deleted from input_items, so `input == 0`
            # alone would spin to timeout whenever a tolerated baseline
            # failure exists).
            if (
                stats.pending == 0
                and stats.output == 0
                and stats.input == stats.failed
            ):
                return stats
            if time.monotonic() >= deadline:
                raise TranslationQueueTimeoutError(
                    timeout_seconds=self.completion_timeout_seconds,
                    stats=stats,
                )
            time.sleep(self.completion_poll_interval_seconds)


def translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    try:
        stats = translator.queue_stats()
    except Exception as error:  # noqa: BLE001 - asset check reports boundary failures
        return dg.AssetCheckResult(passed=False, metadata={"error": str(error)})

    return dg.AssetCheckResult(
        passed=stats.failed == 0,
        metadata=stats.as_metadata(),
    )
