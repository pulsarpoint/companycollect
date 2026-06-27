"""Shared LLM batch translation call used by every source's TranslateWorkflow."""
from __future__ import annotations

from typing import Any

from translator.queue import ClaimedTranslationItem
from translator.types import TranslationInput, TranslationResult


def translate_batch(
    items: list[ClaimedTranslationItem],
    *,
    provider: Any,
    timeout: int,
) -> list[TranslationResult]:
    """Call the LLM provider for a list of claimed queue items.

    Constructs positional provider item_ids (batch-item-NNN) so the provider
    response can be matched back to queue item_ids.  Raises on provider error —
    the caller must call ``queue.fail_batch`` and categorise the exception.

    Args:
        items:    Claimed items from the DuckDB queue.
        provider: Any object with a ``.translate(items, *, timeout_seconds)``
                  method returning ``list[TranslationResult]``.
        timeout:  Per-call timeout in seconds passed to the provider.

    Returns:
        ``TranslationResult`` list with ``item_id`` values matching the
        input ``ClaimedTranslationItem.item_id`` values (queue ids).
    """
    if not items:
        return []

    provider_inputs = [
        TranslationInput(
            item_id=f"batch-item-{index:03d}",
            source_text=item.source_text,
        )
        for index, item in enumerate(items)
    ]
    # Map provider positional ids back to queue ids.
    queue_id_by_provider_id = {
        f"batch-item-{index:03d}": item.item_id
        for index, item in enumerate(items)
    }

    raw_results = provider.translate(provider_inputs, timeout_seconds=timeout)

    return [
        TranslationResult(
            item_id=queue_id_by_provider_id[result.item_id],
            translated_text=result.translated_text,
        )
        for result in raw_results
    ]
