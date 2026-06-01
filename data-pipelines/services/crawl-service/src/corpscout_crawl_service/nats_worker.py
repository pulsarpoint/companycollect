from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Protocol

import nats
from pydantic import ValidationError

from corpscout_crawl_service.models import BrregDomainDiscoveryRequest, DomainDiscoverResponse, ServiceError
from corpscout_crawl_service.mocking import MockCrawlService, mock_enabled_from_env
from corpscout_crawl_service.service import CrawlService


LOGGER = logging.getLogger(__name__)
DEFAULT_NATS_SUBJECT = "brreg.domain.discover"
DEFAULT_NATS_QUEUE = "brreg-domain-crawl"


class NatsMessage(Protocol):
    data: bytes

    async def respond(self, payload: bytes) -> None: ...


class BrregDomainDiscoveryService(Protocol):
    async def discover_brreg_domain(self, request: BrregDomainDiscoveryRequest) -> DomainDiscoverResponse: ...


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


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service: CrawlService | MockCrawlService = MockCrawlService.from_env() if mock_enabled_from_env() else CrawlService()
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    subject = os.getenv("NATS_SUBJECT", DEFAULT_NATS_SUBJECT)
    queue = os.getenv("NATS_QUEUE", DEFAULT_NATS_QUEUE)
    max_concurrent = _positive_int_from_env("CRAWL_WORKER_MAX_CONCURRENT_REQUESTS", 1)
    semaphore = asyncio.Semaphore(max_concurrent)

    await service.start()
    nc = await nats.connect(nats_url)
    LOGGER.info("crawl-service NATS worker connected subject=%s queue=%s max_concurrent=%s", subject, queue, max_concurrent)

    async def callback(message: Any) -> None:
        async with semaphore:
            await handle_brreg_domain_discovery_message(message, service)

    try:
        await nc.subscribe(subject, queue=queue, cb=callback)
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


async def _respond(message: NatsMessage, response: DomainDiscoverResponse) -> None:
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
