from __future__ import annotations

import json

from translations.provider_smoke import run_translation_provider_smoke
from translations.smoke import (
    LocalOpenAICompatibleTranslationProvider,
    build_smoke_translation_prompt,
    parse_smoke_translation_response,
)
from translations.types import SmokeTranslationInput, SmokeTranslationResult


class _SuccessfulProvider:
    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        return [
            SmokeTranslationResult(
                item_id=item.item_id,
                translated_text=f"English {item.item_id}",
            )
            for item in items
        ]


class _FailingProvider:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc or ConnectionRefusedError("connection refused")

    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        raise self.exc


class _FakeChatCompletions:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def create(self, **kwargs: object) -> object:
        self.kwargs = kwargs
        return _FakeOpenAIResponse(
            '{"translations":[{"item_id":"item-00","translated_text":"Public limited company"}]}'
        )


class _FakeOpenAIClient:
    def __init__(self) -> None:
        self.completions = _FakeChatCompletions()
        self.chat = self


class _FakeOpenAIResponse:
    def __init__(self, content: str) -> None:
        self.choices = [_FakeChoice(content)]


class _FakeChoice:
    def __init__(self, content: str) -> None:
        self.message = _FakeMessage(content)


class _FakeMessage:
    def __init__(self, content: str) -> None:
        self.content = content


def test_build_smoke_translation_prompt_disables_qwen_thinking() -> None:
    prompt = build_smoke_translation_prompt(
        [SmokeTranslationInput(item_id="item-00", source_text="Allmennaksjeselskap")]
    )

    assert "/no_think" in prompt
    assert "Do not produce reasoning" in prompt


def test_local_provider_sends_bounded_max_tokens() -> None:
    client = _FakeOpenAIClient()
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url="http://example.test/v1",
        model="qwen3:6b",
        api_key="not-needed",
        client=client,
        max_tokens=256,
    )

    provider.translate(
        [SmokeTranslationInput(item_id="item-00", source_text="Allmennaksjeselskap")],
        timeout_seconds=30,
    )

    assert client.completions.kwargs is not None
    assert client.completions.kwargs["max_tokens"] == 256


def test_local_provider_sends_extra_body_for_thinking_control() -> None:
    client = _FakeOpenAIClient()
    extra_body = {"chat_template_kwargs": {"enable_thinking": False}}
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url="http://example.test/v1",
        model="qwen3:6b",
        api_key="not-needed",
        client=client,
        extra_body=extra_body,
    )

    provider.translate(
        [SmokeTranslationInput(item_id="item-00", source_text="Allmennaksjeselskap")],
        timeout_seconds=30,
    )

    assert client.completions.kwargs is not None
    assert client.completions.kwargs["extra_body"] == extra_body


def test_local_provider_defaults_to_qwen_thinking_disabled_and_large_batch_token_cap() -> None:
    client = _FakeOpenAIClient()
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url="http://example.test/v1",
        model="qwen3:6b",
        api_key="not-needed",
        client=client,
    )

    provider.translate(
        [SmokeTranslationInput(item_id="item-00", source_text="Allmennaksjeselskap")],
        timeout_seconds=30,
    )

    assert client.completions.kwargs is not None
    assert client.completions.kwargs["max_tokens"] == 2048
    assert client.completions.kwargs["extra_body"] == {
        "chat_template_kwargs": {"enable_thinking": False}
    }


def test_run_translation_provider_smoke_records_success_for_each_batch_size() -> None:
    results = run_translation_provider_smoke(
        provider=_SuccessfulProvider(),
        batch_sizes=[1, 5],
        timeout_seconds=30,
    )

    assert [(result.item_count, result.status, result.translated_count) for result in results] == [
        (1, "success", 1),
        (5, "success", 5),
    ]
    assert all(result.duration_seconds >= 0 for result in results)
    assert all(result.error_category is None for result in results)


def test_run_translation_provider_smoke_categorizes_connection_refused() -> None:
    results = run_translation_provider_smoke(
        provider=_FailingProvider(),
        batch_sizes=[1],
        timeout_seconds=30,
    )

    assert len(results) == 1
    assert results[0].status == "failed"
    assert results[0].error_category == "connection_refused"
    assert "connection refused" in results[0].error_message


def test_run_translation_provider_smoke_categorizes_timeout() -> None:
    results = run_translation_provider_smoke(
        provider=_FailingProvider(TimeoutError("request timed out")),
        batch_sizes=[1],
        timeout_seconds=30,
    )

    assert results[0].status == "failed"
    assert results[0].error_category == "timeout"


def test_run_translation_provider_smoke_categorizes_invalid_json() -> None:
    results = run_translation_provider_smoke(
        provider=_FailingProvider(json.JSONDecodeError("Expecting value", "", 0)),
        batch_sizes=[1],
        timeout_seconds=30,
    )

    assert results[0].status == "failed"
    assert results[0].error_category == "invalid_json"


def test_run_translation_provider_smoke_categorizes_missing_item_ids() -> None:
    results = run_translation_provider_smoke(
        provider=_FailingProvider(ValueError("missing item_id values: ['item-01']")),
        batch_sizes=[1],
        timeout_seconds=30,
    )

    assert results[0].status == "failed"
    assert results[0].error_category == "missing_item_ids"


def test_parse_smoke_translation_response_rejects_missing_item_ids() -> None:
    payload = {"translations": [{"item_id": "item-00", "translated_text": "Text 0"}]}

    try:
        parse_smoke_translation_response(
            json.dumps(payload),
            expected_item_ids={"item-00", "item-01"},
        )
    except ValueError as exc:
        assert "missing item_id" in str(exc)
    else:
        raise AssertionError("expected missing item_id validation failure")
