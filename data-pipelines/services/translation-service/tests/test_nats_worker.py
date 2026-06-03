from __future__ import annotations

import json
from typing import Any

import pytest

from corpscout_translation_service.models import BrregTranslateResponse, TermTranslationResponse
from corpscout_translation_service.nats_worker import (
    TERM_RESULT_SUBJECT,
    handle_brreg_term_translation_message,
    handle_brreg_translation_message,
)


TERM_KEY_1 = "6b79e9d3d6b2cfb0c065d83384c1028947fb5f89af7938f4e176122bdd26db72"


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


@pytest.mark.asyncio
async def test_term_nats_handler_publishes_result_response_without_core_nats_ack() -> None:
    service = FakeTermTranslationService(
        TermTranslationResponse(
            request_id="request-1",
            source="brreg",
            source_lang="no",
            target_lang="en",
            provider="fake",
            model="fake-fast",
            prompt_version="v1",
            results=[
                {
                    "term_key": TERM_KEY_1,
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                    "translated_text": "Aksjeselskap EN",
                }
            ],
        )
    )
    publisher = FakeNatsPublisher()
    message = FakeAckNatsMessage(
        {
            "request_id": "request-1",
            "source": "brreg",
            "source_lang": "no",
            "target_lang": "en",
            "provider": "fake",
            "model": "fake-fast",
            "prompt_version": "v1",
            "terms": [
                {
                    "term_key": TERM_KEY_1,
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                }
            ],
        }
    )

    await handle_brreg_term_translation_message(message, service, publisher)

    assert message.ack_count == 0
    assert service.requests[0]["request_id"] == "request-1"
    assert service.requests[0]["terms"][0]["term_key"] == TERM_KEY_1
    assert publisher.published[0][0] == TERM_RESULT_SUBJECT
    body = json.loads(publisher.published[0][1].decode("utf-8"))
    assert body["request_id"] == "request-1"
    assert body["results"][0]["term_key"] == TERM_KEY_1
    assert body["results"][0]["translated_text"] == "Aksjeselskap EN"
    assert body["failures"] == []
    assert len(message.replies) == 1
    reply_body = json.loads(message.replies[0].decode("utf-8"))
    assert reply_body == body


@pytest.mark.asyncio
async def test_term_nats_handler_drops_invalid_term_key_without_core_nats_ack() -> None:
    service = FakeTermTranslationService(
        TermTranslationResponse(
            request_id="request-1",
            source="brreg",
            source_lang="no",
            target_lang="en",
            provider="fake",
            model="fake-fast",
            prompt_version="v1",
        )
    )
    publisher = FakeNatsPublisher()
    message = FakeAckNatsMessage(
        {
            "request_id": "request-1",
            "source": "brreg",
            "source_lang": "no",
            "target_lang": "en",
            "provider": "fake",
            "model": "fake-fast",
            "prompt_version": "v1",
            "terms": [
                {
                    "term_key": "term-1",
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                }
            ],
        }
    )

    await handle_brreg_term_translation_message(message, service, publisher)

    assert message.ack_count == 0
    assert service.requests == []
    assert publisher.published == []


@pytest.mark.asyncio
async def test_term_nats_handler_deduplicates_invalid_payload_failures_by_term_key() -> None:
    service = FakeTermTranslationService(
        TermTranslationResponse(
            request_id="request-1",
            source="brreg",
            source_lang="no",
            target_lang="en",
            provider="fake",
            model="fake-fast",
            prompt_version="v1",
        )
    )
    publisher = FakeNatsPublisher()
    message = FakeAckNatsMessage(
        {
            "request_id": "request-1",
            "source": "brreg",
            "source_lang": "no",
            "target_lang": "en",
            "provider": "fake",
            "model": "fake-fast",
            "prompt_version": "v1",
            "terms": [
                {
                    "term_key": TERM_KEY_1,
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                },
                {
                    "term_key": TERM_KEY_1,
                    "source_text": "Annen tekst",
                    "source_text_normalized": "annen tekst",
                },
            ],
        }
    )

    await handle_brreg_term_translation_message(message, service, publisher)

    assert message.ack_count == 0
    assert service.requests == []
    assert len(publisher.published) == 1
    body = json.loads(publisher.published[0][1].decode("utf-8"))
    assert [failure["term_key"] for failure in body["failures"]] == [TERM_KEY_1]
    assert len(message.replies) == 1
    reply_body = json.loads(message.replies[0].decode("utf-8"))
    assert reply_body == body


class FakeBrregTranslationService:
    def __init__(self, response: BrregTranslateResponse) -> None:
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def translate_brreg_records(self, request: Any) -> BrregTranslateResponse:
        self.requests.append(request.model_dump(mode="json"))
        return self.response


class FakeTermTranslationService:
    def __init__(self, response: TermTranslationResponse) -> None:
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def translate_brreg_terms(self, request: Any) -> TermTranslationResponse:
        self.requests.append(request.model_dump(mode="json"))
        return self.response


class FakeNatsPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        self.published.append((subject, payload))


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


class FakeAckNatsMessage(FakeNatsMessage):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(payload)
        self.ack_count = 0

    async def ack(self) -> None:
        self.ack_count += 1
