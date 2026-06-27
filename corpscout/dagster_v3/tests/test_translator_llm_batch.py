from translator.llm_batch import translate_batch
from translator.queue import ClaimedTranslationItem
from translator.types import TranslationInput, TranslationResult


def _claimed(n: int) -> list[ClaimedTranslationItem]:
    return [
        ClaimedTranslationItem(
            item_id=f"queue-id-{i:02d}",
            batch_id="batch-1",
            source_text=f"tekst {i}",
            target_language="en",
            attempt_count=0,
        )
        for i in range(n)
    ]


class _FakeProvider:
    """Echoes source_text uppercased; tracks calls."""

    called_with: list[list[TranslationInput]] = []

    def translate(
        self,
        items: list[TranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[TranslationResult]:
        _FakeProvider.called_with.append(items)
        return [
            TranslationResult(item_id=item.item_id, translated_text=item.source_text.upper())
            for item in items
        ]


def test_translate_batch_maps_results_to_queue_ids():
    _FakeProvider.called_with = []
    items = _claimed(3)
    provider = _FakeProvider()

    results = translate_batch(items, provider=provider, timeout=30)

    # Results must be keyed by QUEUE item_id, not positional provider ids.
    assert len(results) == 3
    result_by_id = {r.item_id: r for r in results}
    for i, item in enumerate(items):
        assert item.item_id in result_by_id
        assert result_by_id[item.item_id].translated_text == f"TEKST {i}"


def test_translate_batch_calls_provider_once():
    _FakeProvider.called_with = []
    items = _claimed(2)
    translate_batch(items, provider=_FakeProvider(), timeout=30)
    assert len(_FakeProvider.called_with) == 1
    # Provider sees positional ids (batch-item-00, batch-item-01), not queue ids.
    sent_ids = {item.item_id for item in _FakeProvider.called_with[0]}
    queue_ids = {item.item_id for item in items}
    assert sent_ids.isdisjoint(queue_ids)  # provider ids differ from queue ids


def test_translate_batch_propagates_provider_error():
    class _ErrorProvider:
        def translate(self, items, *, timeout_seconds):
            raise RuntimeError("LLM unreachable")

    import pytest
    with pytest.raises(RuntimeError, match="LLM unreachable"):
        translate_batch(_claimed(1), provider=_ErrorProvider(), timeout=30)


def test_translate_batch_empty_items_returns_empty():
    results = translate_batch([], provider=_FakeProvider(), timeout=30)
    assert results == []
