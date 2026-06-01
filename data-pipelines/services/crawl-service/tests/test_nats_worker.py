from __future__ import annotations

import json
from typing import Any

import pytest

from corpscout_crawl_service.models import (
    Crawl4AiResponse,
    DomainDiscoverResponse,
    PageAnalyzeResponse,
    SearchAnalyzeResponse,
    ScoredLink,
)
from corpscout_crawl_service.nats_worker import (
    handle_brreg_domain_discovery_message,
    handle_page_analyze_message,
    handle_page_crawl_message,
    handle_search_analyze_message,
    handle_search_fetch_message,
)


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
            "llm": {
                "provider": "deepseek-v4-flash",
                "model": "deepseek-v4-flash",
                "base_url": "https://api.deepseek.com",
                "api_key": "secret-key",
            },
        }
    )

    await handle_brreg_domain_discovery_message(message, service)

    assert service.requests[0]["organization_number"] == "810202572"
    assert service.requests[0]["llm"] == {
        "provider": "deepseek-v4-flash",
        "model": "deepseek-v4-flash",
        "base_url": "https://api.deepseek.com",
        "api_key": "secret-key",
    }
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


@pytest.mark.asyncio
async def test_search_fetch_nats_handler_replies_with_search_page_artifact() -> None:
    service = FakeSearchActionService()
    message = FakeNatsMessage(
        {
            "search_term": "BORTIGARD AS NO website",
            "search_engine": "duckduckgo",
            "timeout_seconds": 60,
        }
    )

    await handle_search_fetch_message(message, service)

    assert service.fetch_requests == [
        {
            "search_term": "BORTIGARD AS NO website",
            "search_engine": "duckduckgo",
            "timeout_seconds": 60,
        }
    ]
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "succeeded"
    assert body["markdown_hash"] == "search-hash"
    assert body["links"] == ["https://bortigard.no/"]


@pytest.mark.asyncio
async def test_search_analyze_nats_handler_replies_with_candidates() -> None:
    service = FakeSearchActionService()
    message = FakeNatsMessage(
        {
            "company_name": "BORTIGARD AS",
            "organization_number": "810202572",
            "country": "NO",
            "search_engine": "duckduckgo",
            "search_term": "BORTIGARD AS NO website",
            "links": ["https://bortigard.no/"],
            "markdown": "# Search results",
            "llm": {
                "provider": "deepseek-v4-flash",
                "model": "deepseek-v4-flash",
                "base_url": "https://api.deepseek.com",
                "api_key": "secret-key",
            },
        }
    )

    await handle_search_analyze_message(message, service)

    assert service.analyze_requests[0]["company_name"] == "BORTIGARD AS"
    assert service.analyze_requests[0]["llm"] == {
        "provider": "deepseek-v4-flash",
        "model": "deepseek-v4-flash",
        "base_url": "https://api.deepseek.com",
        "api_key": "secret-key",
    }
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "succeeded"
    assert body["candidates"][0]["normalized_domain"] == "bortigard.no"
    assert body["candidates"][0]["score"] == 88


@pytest.mark.asyncio
async def test_search_fetch_nats_handler_returns_structured_error_for_invalid_payload() -> None:
    service = FakeSearchActionService()
    message = FakeRawNatsMessage(b"{not-json")

    await handle_search_fetch_message(message, service)

    assert service.fetch_requests == []
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "failed"
    assert body["error"]["code"] == "invalid_nats_payload"


@pytest.mark.asyncio
async def test_page_crawl_nats_handler_replies_with_site_crawl_artifact() -> None:
    service = FakeSearchActionService()
    message = FakeNatsMessage(
        {
            "url": "https://bortigard.no/",
            "timeout_seconds": 60,
            "metadata": {"candidate_score": 88},
        }
    )

    await handle_page_crawl_message(message, service)

    assert service.page_crawl_requests == [
        {
            "url": "https://bortigard.no/",
            "timeout_seconds": 60,
            "metadata": {"candidate_score": 88},
        }
    ]
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "succeeded"
    assert body["markdown_hash"] == "site-hash"


@pytest.mark.asyncio
async def test_page_analyze_nats_handler_replies_with_site_analysis() -> None:
    service = FakeSearchActionService()
    message = FakeNatsMessage(
        {
            "company_name": "BORTIGARD AS",
            "organization_number": "810202572",
            "country": "NO",
            "url": "https://bortigard.no/",
            "final_url": "https://bortigard.no/",
            "normalized_domain": "bortigard.no",
            "markdown": "# BORTIGARD AS",
            "candidate_score": 88,
            "candidate_reason": "Search result matches.",
            "llm": {
                "provider": "deepseek-v4-flash",
                "model": "deepseek-v4-flash",
                "base_url": "https://api.deepseek.com",
                "api_key": "secret-key",
            },
        }
    )

    await handle_page_analyze_message(message, service)

    assert service.page_analyze_requests[0]["company_name"] == "BORTIGARD AS"
    assert service.page_analyze_requests[0]["llm"] == {
        "provider": "deepseek-v4-flash",
        "model": "deepseek-v4-flash",
        "base_url": "https://api.deepseek.com",
        "api_key": "secret-key",
    }
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "succeeded"
    assert body["analysis"]["decision"] == "accepted"
    assert body["analysis"]["owned_domain"] is True


@pytest.mark.asyncio
async def test_page_analyze_nats_handler_returns_structured_error_for_invalid_payload() -> None:
    service = FakeSearchActionService()
    message = FakeRawNatsMessage(b"{not-json")

    await handle_page_analyze_message(message, service)

    assert service.page_analyze_requests == []
    body = json.loads(message.replies[0].decode("utf-8"))
    assert body["status"] == "failed"
    assert body["error"]["code"] == "invalid_nats_payload"


class FakeBrregDomainService:
    def __init__(self, response: DomainDiscoverResponse) -> None:
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def discover_brreg_domain(self, request: Any) -> DomainDiscoverResponse:
        self.requests.append(request.model_dump(mode="json"))
        return self.response


class FakeSearchActionService:
    def __init__(self) -> None:
        self.fetch_requests: list[dict[str, Any]] = []
        self.analyze_requests: list[dict[str, Any]] = []
        self.page_crawl_requests: list[dict[str, Any]] = []
        self.page_analyze_requests: list[dict[str, Any]] = []

    async def fetch_search_page(self, request: Any) -> Crawl4AiResponse:
        self.fetch_requests.append(request.model_dump(mode="json"))
        return Crawl4AiResponse(
            url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
            final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
            status="succeeded",
            markdown="# Search results",
            markdown_hash="search-hash",
            links=["https://bortigard.no/"],
            duration_ms=10,
        )

    async def analyze_search_page(self, request: Any) -> SearchAnalyzeResponse:
        self.analyze_requests.append(request.model_dump(mode="json"))
        return SearchAnalyzeResponse(
            status="succeeded",
            candidates=[
                ScoredLink(
                    url="https://bortigard.no/",
                    domain="bortigard.no",
                    normalized_domain="bortigard.no",
                    score=88,
                    reason="Search result matches.",
                )
            ],
        )

    async def crawl_page(self, request: Any) -> Crawl4AiResponse:
        self.page_crawl_requests.append(request.model_dump(mode="json"))
        return Crawl4AiResponse(
            url="https://bortigard.no/",
            final_url="https://bortigard.no/",
            status="succeeded",
            markdown="# BORTIGARD AS",
            markdown_hash="site-hash",
            links=["https://bortigard.no/contact"],
            duration_ms=9,
        )

    async def analyze_company_page(self, request: Any) -> PageAnalyzeResponse:
        self.page_analyze_requests.append(request.model_dump(mode="json"))
        return PageAnalyzeResponse(
            status="succeeded",
            analysis={
                "decision": "accepted",
                "score": 91,
                "site_type": "company_website",
                "relationship": "primary_web_presence",
                "owned_domain": True,
                "reason": "The page names BORTIGARD AS.",
                "evidence": ["BORTIGARD AS"],
            },
        )


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
