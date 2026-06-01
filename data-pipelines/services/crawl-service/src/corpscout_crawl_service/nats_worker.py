from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Protocol

import nats
from pydantic import BaseModel
from pydantic import ValidationError

from corpscout_crawl_service.models import (
    BrregDomainDiscoveryRequest,
    Crawl4AiResponse,
    DomainDiscoverResponse,
    PageAnalyzeRequest,
    PageAnalyzeResponse,
    PageCrawlRequest,
    SearchAnalyzeRequest,
    SearchAnalyzeResponse,
    SearchFetchRequest,
    ServiceError,
)
from corpscout_crawl_service.mocking import MockCrawlService, mock_enabled_from_env
from corpscout_crawl_service.service import CrawlService


LOGGER = logging.getLogger(__name__)
DEFAULT_NATS_SUBJECT = "brreg.domain.discover"
DEFAULT_SEARCH_FETCH_SUBJECT = "brreg.domain.search.fetch"
DEFAULT_SEARCH_ANALYZE_SUBJECT = "brreg.domain.search.analyze"
DEFAULT_PAGE_CRAWL_SUBJECT = "brreg.domain.page.crawl"
DEFAULT_PAGE_ANALYZE_SUBJECT = "brreg.domain.page.analyze"
DEFAULT_NATS_QUEUE = "brreg-domain-crawl"


class NatsMessage(Protocol):
    data: bytes

    async def respond(self, payload: bytes) -> None: ...


class BrregDomainDiscoveryService(Protocol):
    async def discover_brreg_domain(self, request: BrregDomainDiscoveryRequest) -> DomainDiscoverResponse: ...


class SearchActionService(Protocol):
    async def fetch_search_page(self, request: SearchFetchRequest) -> Crawl4AiResponse: ...

    async def analyze_search_page(self, request: SearchAnalyzeRequest) -> SearchAnalyzeResponse: ...

    async def crawl_page(self, request: PageCrawlRequest) -> Crawl4AiResponse: ...

    async def analyze_company_page(self, request: PageAnalyzeRequest) -> PageAnalyzeResponse: ...


async def handle_brreg_domain_discovery_message(
    message: NatsMessage,
    service: BrregDomainDiscoveryService,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = BrregDomainDiscoveryRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        await _respond(message, _failed_response("invalid_nats_payload", "Invalid BRREG domain discovery request.", exc))
        return

    try:
        response = await service.discover_brreg_domain(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("BRREG domain discovery NATS handler failed")
        response = _failed_response("crawl_worker_error", "BRREG domain discovery worker failed.", exc)

    await _respond(message, response)


async def handle_search_fetch_message(
    message: NatsMessage,
    service: SearchActionService,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = SearchFetchRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        await _respond(message, _failed_crawl_response("invalid_nats_payload", "Invalid search fetch request.", exc))
        return

    try:
        response = await service.fetch_search_page(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("search fetch NATS handler failed")
        response = _failed_crawl_response("crawl_worker_error", "Search fetch worker failed.", exc)

    await _respond(message, response)


async def handle_search_analyze_message(
    message: NatsMessage,
    service: SearchActionService,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = SearchAnalyzeRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        await _respond(
            message,
            _failed_search_analyze_response("invalid_nats_payload", "Invalid search analysis request.", exc),
        )
        return

    try:
        response = await service.analyze_search_page(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("search analysis NATS handler failed")
        response = _failed_search_analyze_response("crawl_worker_error", "Search analysis worker failed.", exc)

    await _respond(message, response)


async def handle_page_crawl_message(
    message: NatsMessage,
    service: SearchActionService,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = PageCrawlRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        await _respond(message, _failed_crawl_response("invalid_nats_payload", "Invalid page crawl request.", exc))
        return

    try:
        response = await service.crawl_page(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("page crawl NATS handler failed")
        response = _failed_crawl_response("crawl_worker_error", "Page crawl worker failed.", exc)

    await _respond(message, response)


async def handle_page_analyze_message(
    message: NatsMessage,
    service: SearchActionService,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = PageAnalyzeRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        await _respond(
            message,
            _failed_page_analyze_response("invalid_nats_payload", "Invalid page analysis request.", exc),
        )
        return

    try:
        response = await service.analyze_company_page(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("page analysis NATS handler failed")
        response = _failed_page_analyze_response("crawl_worker_error", "Page analysis worker failed.", exc)

    await _respond(message, response)


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service: CrawlService | MockCrawlService = MockCrawlService.from_env() if mock_enabled_from_env() else CrawlService()
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    domain_discovery_subject = os.getenv(
        "CRAWL_NATS_BRREG_DOMAIN_DISCOVERY_SUBJECT",
        os.getenv("NATS_SUBJECT", DEFAULT_NATS_SUBJECT),
    )
    search_fetch_subject = os.getenv("CRAWL_NATS_SEARCH_FETCH_SUBJECT", DEFAULT_SEARCH_FETCH_SUBJECT)
    search_analyze_subject = os.getenv("CRAWL_NATS_SEARCH_ANALYZE_SUBJECT", DEFAULT_SEARCH_ANALYZE_SUBJECT)
    page_crawl_subject = os.getenv("CRAWL_NATS_PAGE_CRAWL_SUBJECT", DEFAULT_PAGE_CRAWL_SUBJECT)
    page_analyze_subject = os.getenv("CRAWL_NATS_PAGE_ANALYZE_SUBJECT", DEFAULT_PAGE_ANALYZE_SUBJECT)
    queue = os.getenv("NATS_QUEUE", DEFAULT_NATS_QUEUE)
    max_concurrent = _positive_int_from_env("CRAWL_WORKER_MAX_CONCURRENT_REQUESTS", 1)
    semaphore = asyncio.Semaphore(max_concurrent)

    await service.start()
    nc = await nats.connect(nats_url)
    LOGGER.info(
        "crawl-service NATS worker connected subjects=%s queue=%s max_concurrent=%s",
        ",".join(
            _configured_subjects(
                domain_discovery_subject,
                search_fetch_subject,
                search_analyze_subject,
                page_crawl_subject,
                page_analyze_subject,
            )
        ),
        queue,
        max_concurrent,
    )

    async def domain_callback(message: Any) -> None:
        async with semaphore:
            await handle_brreg_domain_discovery_message(message, service)

    async def search_fetch_callback(message: Any) -> None:
        async with semaphore:
            await handle_search_fetch_message(message, service)

    async def search_analyze_callback(message: Any) -> None:
        async with semaphore:
            await handle_search_analyze_message(message, service)

    async def page_crawl_callback(message: Any) -> None:
        async with semaphore:
            await handle_page_crawl_message(message, service)

    async def page_analyze_callback(message: Any) -> None:
        async with semaphore:
            await handle_page_analyze_message(message, service)

    try:
        if domain_discovery_subject:
            await nc.subscribe(domain_discovery_subject, queue=queue, cb=domain_callback)
        if search_fetch_subject:
            await nc.subscribe(search_fetch_subject, queue=queue, cb=search_fetch_callback)
        if search_analyze_subject:
            await nc.subscribe(search_analyze_subject, queue=queue, cb=search_analyze_callback)
        if page_crawl_subject:
            await nc.subscribe(page_crawl_subject, queue=queue, cb=page_crawl_callback)
        if page_analyze_subject:
            await nc.subscribe(page_analyze_subject, queue=queue, cb=page_analyze_callback)
        while True:
            await asyncio.sleep(3600)
    finally:
        await nc.drain()
        await service.close()


def _failed_response(code: str, message: str, exc: Exception) -> DomainDiscoverResponse:
    return DomainDiscoverResponse(
        status="failed",
        duration_ms=0,
        errors=[
            ServiceError(
                code=code,
                message=message,
                category="crawl_service",
                retry_strategy="retry_with_backoff",
                detail={"error": str(exc)},
            )
        ],
    )


def _failed_crawl_response(code: str, message: str, exc: Exception) -> Crawl4AiResponse:
    return Crawl4AiResponse(
        url="",
        final_url="",
        status="failed",
        error={
            "code": code,
            "message": message,
            "category": "crawl_service",
            "retry_strategy": "retry_with_backoff",
            "detail": {"error": str(exc)},
        },
        duration_ms=0,
    )


def _failed_search_analyze_response(code: str, message: str, exc: Exception) -> SearchAnalyzeResponse:
    return SearchAnalyzeResponse(
        status="failed",
        error=ServiceError(
            code=code,
            message=message,
            category="crawl_service",
            retry_strategy="retry_with_backoff",
            detail={"error": str(exc)},
        ),
    )


def _failed_page_analyze_response(code: str, message: str, exc: Exception) -> PageAnalyzeResponse:
    return PageAnalyzeResponse(
        status="failed",
        error=ServiceError(
            code=code,
            message=message,
            category="crawl_service",
            retry_strategy="retry_with_backoff",
            detail={"error": str(exc)},
        ),
    )


async def _respond(message: NatsMessage, response: BaseModel) -> None:
    payload = response.model_dump_json(exclude_none=True).encode("utf-8")
    await message.respond(payload)


def _positive_int_from_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _configured_subjects(*subjects: str | None) -> list[str]:
    return [subject for subject in subjects if subject]
