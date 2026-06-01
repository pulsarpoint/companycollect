from __future__ import annotations

import json
from typing import Any

import pytest

from corpscout_translation_service.models import BrregTranslateResponse
from corpscout_translation_service.nats_worker import handle_brreg_translation_message


@pytest.mark.asyncio
async def test_nats_handler_replies_with_brreg_translation_response() -> None:
    service = FakeBrregTranslationService(
        BrregTranslateResponse(
            status="succeeded",
            provider="mock",
            model="mock-fast",
            prompt_version="v1",
            records_seen=1,
            records_completed=1,
            records_failed=0,
            records_skipped=0,
            duration_ms=12,
            results=[],
        )
    )
    message = FakeNatsMessage(
        {
            "records": [
                {
                    "record_id": "raw-1",
                    "organization_number": "810202572",
                    "raw_payload": {"navn": "BORTIGARD AS"},
                }
            ],
            "llm": {
                "provider": "mock",
                "model": "mock-fast",
                "base_url": "https://llm.example",
                "api_key": "secret-key",
            },
            "prompt_version": "v1",
            "source_lang": "no",
            "target_lang": "en",
            "max_retries": 3,
        }
    )

    await handle_brreg_translation_message(message, service)

    assert service.requests[0]["records"][0]["organization_number"] == "810202572"
    assert service.requests[0]["llm"] == {
        "provider": "mock",
        "model": "mock-fast",
        "base_url": "https://llm.example",
        "api_key": "secret-key",
    }
    assert len(message.replies) == 1
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "succeeded"
    assert body["provider"] == "mock"


@pytest.mark.asyncio
async def test_nats_handler_returns_structured_error_for_invalid_payload() -> None:
    service = FakeBrregTranslationService(
        BrregTranslateResponse(
            status="succeeded",
            provider="mock",
            model="mock-fast",
            prompt_version="v1",
            records_seen=0,
            records_completed=0,
            records_failed=0,
            records_skipped=0,
            duration_ms=1,
            results=[],
        )
    )
    message = FakeRawNatsMessage(b"{not-json")

    await handle_brreg_translation_message(message, service)

    assert service.requests == []
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "failed"
    assert body["results"][0]["status"] == "failed"
    assert body["results"][0]["error"]["code"] == "invalid_nats_payload"


class FakeBrregTranslationService:
    def __init__(self, response: BrregTranslateResponse) -> None:
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def translate_brreg_records(self, request: Any) -> BrregTranslateResponse:
        self.requests.append(request.model_dump(mode="json"))
        return self.response


class FakeNatsMessage:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.data = json.dumps(payload).encode("utf-8")
        self.replies: list[bytes] = []

    async def respond(self, payload: bytes) -> None:
        self.replies.append(payload)


class FakeRawNatsMessage(FakeNatsMessage):
    def __init__(self, payload: bytes) -> None:
        self.data = payload
        self.replies: list[bytes] = []
