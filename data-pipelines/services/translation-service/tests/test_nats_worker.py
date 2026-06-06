from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from corpscout_translation_service.models import (
    BrregTranslateResponse,
    TermTranslationResponse,
    TermTranslationResultItem,
)
from corpscout_translation_service.nats_worker import (
    handle_brreg_term_translation_message,
    handle_brreg_translation_message,
    handle_jetstream_translation_job,
    run_jetstream_translation_loop,
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
async def test_term_nats_handler_replies_with_result_response_without_core_nats_ack() -> None:
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

    await handle_brreg_term_translation_message(message, service)

    assert message.ack_count == 0
    assert service.requests[0]["request_id"] == "request-1"
    assert service.requests[0]["terms"][0]["term_key"] == TERM_KEY_1
    assert len(message.replies) == 1
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["request_id"] == "request-1"
    assert body["results"][0]["term_key"] == TERM_KEY_1
    assert body["results"][0]["translated_text"] == "Aksjeselskap EN"
    assert body["failures"] == []


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

    await handle_brreg_term_translation_message(message, service)

    assert message.ack_count == 0
    assert service.requests == []
    assert message.replies == []


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

    await handle_brreg_term_translation_message(message, service)

    assert message.ack_count == 0
    assert service.requests == []
    assert len(message.replies) == 1
    body = json.loads(message.replies[0].decode("utf-8"))
    assert [failure["term_key"] for failure in body["failures"]] == [TERM_KEY_1]


@pytest.mark.asyncio
async def test_jetstream_translation_job_acks_before_publishing_result() -> None:
    message = FakeJetStreamMessage(_jetstream_job_payload(job_id="job-1", batch_id="batch-1"))
    publisher = FakeResultPublisher()
    service = FakeJetStreamTermService(translated_text="Limited liability company", assert_message_acked=message)

    await handle_jetstream_translation_job(message, service, publisher)

    assert message.ack_count == 1
    assert publisher.results[0]["job_id"] == "job-1"
    assert publisher.results[0]["batch_id"] == "batch-1"
    assert publisher.results[0]["status"] == "succeeded"
    assert publisher.results[0]["company_ids"] == ["company-a"]
    assert publisher.results[0]["results"][0]["translated_text"] == "Limited liability company"


@pytest.mark.asyncio
async def test_jetstream_translation_loop_fetches_one_message_at_a_time() -> None:
    first = FakeJetStreamMessage(_jetstream_job_payload(job_id="job-1", batch_id="batch-1"))
    second = FakeJetStreamMessage(_jetstream_job_payload(job_id="job-2", batch_id="batch-2"))
    subscription = FakePullSubscription([first, second])
    publisher = FakeResultPublisher()
    service = FakeJetStreamTermService(translated_text="Translated")

    with pytest.raises(asyncio.CancelledError):
        await run_jetstream_translation_loop(
            subscription,
            service,
            publisher,
            semaphore=asyncio.Semaphore(1),
            fetch_timeout_seconds=0.01,
        )

    assert subscription.fetch_calls == [(1, 0.01), (1, 0.01), (1, 0.01)]
    assert service.request_ids == ["job-1", "job-2"]
    assert service.max_active_requests == 1
    assert [result["job_id"] for result in publisher.results] == ["job-1", "job-2"]
    assert first.ack_count == 1
    assert second.ack_count == 1


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


class FakeJetStreamMessage(FakeNatsMessage):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(payload)
        self.ack_count = 0

    async def ack(self) -> None:
        self.ack_count += 1


class FakePullSubscription:
    def __init__(self, messages: list[FakeJetStreamMessage]) -> None:
        self._messages = messages
        self.fetch_calls: list[tuple[int, float]] = []

    async def fetch(self, *, batch: int, timeout: float) -> list[FakeJetStreamMessage]:
        self.fetch_calls.append((batch, timeout))
        if not self._messages:
            raise asyncio.CancelledError
        return [self._messages.pop(0)]


class FakeResultPublisher:
    def __init__(self) -> None:
        self.results: list[dict[str, Any]] = []

    async def publish_result(self, result: Any) -> None:
        self.results.append(result.model_dump(exclude_none=True))


class FakeJetStreamTermService:
    def __init__(
        self,
        *,
        translated_text: str,
        assert_message_acked: FakeJetStreamMessage | None = None,
    ) -> None:
        self.translated_text = translated_text
        self.assert_message_acked = assert_message_acked
        self.request_ids: list[str] = []
        self.active_requests = 0
        self.max_active_requests = 0

    async def translate_brreg_terms(self, request: Any) -> TermTranslationResponse:
        if self.assert_message_acked is not None:
            assert self.assert_message_acked.ack_count == 1
        self.active_requests += 1
        self.max_active_requests = max(self.max_active_requests, self.active_requests)
        self.request_ids.append(request.request_id)
        await asyncio.sleep(0)
        self.active_requests -= 1
        return TermTranslationResponse(
            request_id=request.request_id,
            source=request.source,
            source_lang=request.source_lang,
            target_lang=request.target_lang,
            provider=request.provider,
            model=request.model,
            prompt_version=request.prompt_version,
            results=[
                TermTranslationResultItem(
                    term_key=request.terms[0].term_key,
                    source_text=request.terms[0].source_text,
                    source_text_normalized=request.terms[0].source_text_normalized,
                    translated_text=self.translated_text,
                )
            ],
        )


def _jetstream_job_payload(*, job_id: str, batch_id: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "batch_id": batch_id,
        "source": "brreg",
        "source_lang": "no",
        "target_lang": "en",
        "provider": "default",
        "model": "deepseek-chat",
        "prompt_version": "v1",
        "company_ids": ["company-a"],
        "terms": [
            {
                "term_key": TERM_KEY_1,
                "source_text": "Aksjeselskap",
                "source_text_normalized": "aksjeselskap",
            }
        ],
    }
