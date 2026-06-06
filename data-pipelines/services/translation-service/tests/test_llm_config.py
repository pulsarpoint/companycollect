from __future__ import annotations

from typing import Any

import pytest

from corpscout_translation_service.llm import OpenAICompatibleLLMClient, normalize_openai_api_base
from corpscout_translation_service.models import LLMTranslationItem, LLMTranslationRequest


def test_normalize_openai_api_base_appends_v1() -> None:
    assert normalize_openai_api_base("http://llm.example:8888") == "http://llm.example:8888/v1"


def test_normalize_openai_api_base_accepts_existing_v1_or_chat_completion_url() -> None:
    assert normalize_openai_api_base("http://llm.example:8888/v1") == "http://llm.example:8888/v1"
    assert (
        normalize_openai_api_base("http://llm.example:8888/v1/chat/completions")
        == "http://llm.example:8888/v1"
    )


@pytest.mark.asyncio
async def test_openai_client_reuses_http_client_between_translation_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    created_clients: list[CountingAsyncClient] = []

    class CountingAsyncClient:
        def __init__(self, **_: Any) -> None:
            self.posts = 0
            self.closed = False
            created_clients.append(self)

        async def __aenter__(self) -> "CountingAsyncClient":
            return self

        async def __aexit__(self, *_: Any) -> None:
            self.closed = True

        async def post(self, *_: Any, **__: Any) -> FakeHTTPResponse:
            self.posts += 1
            return FakeHTTPResponse()

        async def aclose(self) -> None:
            self.closed = True

    class FakeHTTPResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {
                "choices": [
                    {
                        "message": {
                            "content": '{"translations":[{"id":"t001","translation":"Limited liability company"}]}'
                        }
                    }
                ]
            }

    monkeypatch.setattr("corpscout_translation_service.llm.httpx.AsyncClient", CountingAsyncClient)
    client = OpenAICompatibleLLMClient(timeout_seconds=1)
    request = LLMTranslationRequest(
        provider="default",
        model="qwen3:6b",
        prompt_version="v1",
        source_lang="et",
        target_lang="en",
        items=[LLMTranslationItem(id="t001", category="company_form", text="Osaühing")],
    )

    await client.translate_terms(request)
    await client.translate_terms(request)

    assert len(created_clients) == 1
    assert created_clients[0].posts == 2
    assert not created_clients[0].closed

    await client.aclose()
    assert created_clients[0].closed
