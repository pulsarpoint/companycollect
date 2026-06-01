from __future__ import annotations

import json
from typing import Any

import pytest

from corpscout_crawl_service.models import DomainDiscoverResponse
from corpscout_crawl_service.nats_worker import handle_brreg_domain_discovery_message


@pytest.mark.asyncio
async def test_nats_handler_replies_with_brreg_domain_response() -> None:
    service = FakeBrregDomainService(
        DomainDiscoverResponse(
            status="succeeded",
            best_domain="bortigard.no",
            search_engine="duckduckgo",
            duration_ms=12,
        )
    )
    message = FakeNatsMessage(
        {
            "record_id": "raw-1",
            "organization_number": "810202572",
            "organization_name": "BORTIGARD AS",
            "raw_payload": {"navn": "BORTIGARD AS"},
            "country": "NO",
            "search_provider": "duckduckgo",
            "prompt_version": "v1",
        }
    )

    await handle_brreg_domain_discovery_message(message, service)

    assert service.requests[0]["organization_number"] == "810202572"
    assert len(message.replies) == 1
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "succeeded"
    assert body["best_domain"] == "bortigard.no"


@pytest.mark.asyncio
async def test_nats_handler_returns_structured_error_for_invalid_payload() -> None:
    service = FakeBrregDomainService(
        DomainDiscoverResponse(status="succeeded", best_domain="unused.no", duration_ms=1)
    )
    message = FakeRawNatsMessage(b"{not-json")

    await handle_brreg_domain_discovery_message(message, service)

    assert service.requests == []
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "failed"
    assert body["errors"][0]["code"] == "invalid_nats_payload"


class FakeBrregDomainService:
    def __init__(self, response: DomainDiscoverResponse) -> None:
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def discover_brreg_domain(self, request: Any) -> DomainDiscoverResponse:
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
