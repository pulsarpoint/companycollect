from __future__ import annotations

import dataclasses
from dataclasses import asdict

import pytest

from temporal.translations.queue import (
    InitializeTranslationQueueInput,
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    ProcessTranslationBatchInput,
    ProcessTranslationBatchResult,
    TranslationQueueWorkflowInput,
    initialize_translation_queue_once,
    process_translation_batch_once,
)
from translations.queue import TranslationQueue
from translations.queue_smoke import generate_synthetic_translation_items
from translations.types import SmokeTranslationInput, SmokeTranslationResult


class _SuccessfulProvider:
    def __init__(self) -> None:
        self.seen_item_ids: list[list[str]] = []

    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        self.seen_item_ids.append([item.item_id for item in items])
        return [
            SmokeTranslationResult(
                item_id=item.item_id,
                translated_text=f"English {item.source_text}",
            )
            for item in items
        ]


class _FailingProvider:
    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        raise ValueError("invalid json")


EXTRA_BODY_JSON = '{"chat_template_kwargs":{"enable_thinking":false}}'


def test_translation_queue_workflow_input_requires_explicit_runtime_values() -> None:
    field_defaults = {
        field.name: field.default
        for field in dataclasses.fields(TranslationQueueWorkflowInput)
    }

    assert set(field_defaults) == {
        "duckdb_path",
        "batch_size",
        "timeout_seconds",
        "max_batch_failures",
        "worker_id",
        "max_tokens",
        "extra_body_json",
        "initialize_timeout_seconds",
        "batch_timeout_buffer_seconds",
        "summarize_timeout_seconds",
        "activity_maximum_attempts",
    }
    assert all(default is dataclasses.MISSING for default in field_defaults.values())

    with pytest.raises(TypeError):
        TranslationQueueWorkflowInput(
            duckdb_path="/tmp/translations.duckdb",
        )

    params = TranslationQueueWorkflowInput(
        duckdb_path="/tmp/translations.duckdb",
        batch_size=50,
        timeout_seconds=120,
        max_batch_failures=0,
        worker_id="translation-temporal-worker",
        max_tokens=4096,
        extra_body_json=EXTRA_BODY_JSON,
        initialize_timeout_seconds=300,
        batch_timeout_buffer_seconds=30,
        summarize_timeout_seconds=30,
        activity_maximum_attempts=1,
    )

    assert asdict(params) == {
        "duckdb_path": "/tmp/translations.duckdb",
        "batch_size": 50,
        "timeout_seconds": 120,
        "max_batch_failures": 0,
        "worker_id": "translation-temporal-worker",
        "max_tokens": 4096,
        "extra_body_json": EXTRA_BODY_JSON,
        "initialize_timeout_seconds": 300,
        "batch_timeout_buffer_seconds": 30,
        "summarize_timeout_seconds": 30,
        "activity_maximum_attempts": 1,
    }
    assert LOCAL_LLM_TRANSLATION_TASK_QUEUE == "translation-local-llm"


def test_initialize_translation_queue_once_only_initializes_tables(tmp_path) -> None:
    duckdb_path = tmp_path / "translations.duckdb"

    count = initialize_translation_queue_once(
        InitializeTranslationQueueInput(duckdb_path=str(duckdb_path))
    )

    assert count == 0
    queue = TranslationQueue(duckdb_path)
    summary = queue.summary()
    assert summary.total_items == 0


def test_process_translation_batch_once_completes_one_batch(tmp_path) -> None:
    duckdb_path = tmp_path / "translations.duckdb"
    queue = TranslationQueue(duckdb_path)
    queue.initialize()
    queue.enqueue_items(generate_synthetic_translation_items(60, source_duckdb_path=str(duckdb_path)))
    provider = _SuccessfulProvider()

    result = process_translation_batch_once(
        ProcessTranslationBatchInput(
            duckdb_path=str(duckdb_path),
            batch_size=50,
            timeout_seconds=30,
            worker_id="worker-a",
            max_tokens=4096,
            extra_body_json=EXTRA_BODY_JSON,
        ),
        provider=provider,
    )

    assert result == ProcessTranslationBatchResult(
        status="success",
        item_count=50,
        duration_seconds=result.duration_seconds,
        error_category=None,
        error_message=None,
    )
    assert provider.seen_item_ids == [[f"batch-item-{index:03d}" for index in range(50)]]
    summary = queue.summary()
    assert summary.completed_items == 50
    assert summary.pending_items == 10
    assert summary.failed_batches == 0


def test_process_translation_batch_once_marks_failed_batch_retryable(tmp_path) -> None:
    duckdb_path = tmp_path / "translations.duckdb"
    queue = TranslationQueue(duckdb_path)
    queue.initialize()
    queue.enqueue_items(generate_synthetic_translation_items(60, source_duckdb_path=str(duckdb_path)))

    result = process_translation_batch_once(
        ProcessTranslationBatchInput(
            duckdb_path=str(duckdb_path),
            batch_size=50,
            timeout_seconds=30,
            worker_id="worker-a",
            max_tokens=4096,
            extra_body_json=EXTRA_BODY_JSON,
        ),
        provider=_FailingProvider(),
    )

    assert result.status == "failed"
    assert result.item_count == 50
    assert result.error_category == "invalid_json"
    summary = queue.summary()
    assert summary.completed_items == 0
    assert summary.failed_retryable_items == 50
    assert summary.failed_batches == 1


def test_process_translation_batch_once_returns_empty_when_no_work(tmp_path) -> None:
    duckdb_path = tmp_path / "translations.duckdb"
    queue = TranslationQueue(duckdb_path)
    queue.initialize()

    result = process_translation_batch_once(
        ProcessTranslationBatchInput(
            duckdb_path=str(duckdb_path),
            batch_size=50,
            timeout_seconds=30,
            worker_id="worker-a",
            max_tokens=4096,
            extra_body_json=EXTRA_BODY_JSON,
        ),
        provider=_SuccessfulProvider(),
    )

    assert result.status == "empty"
    assert result.item_count == 0
