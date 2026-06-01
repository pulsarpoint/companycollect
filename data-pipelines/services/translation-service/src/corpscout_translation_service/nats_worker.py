from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Protocol

import nats
from pydantic import ValidationError

from corpscout_translation_service.models import (
    BrregRecordTranslationResult,
    BrregTranslateRequest,
    BrregTranslateResponse,
    TranslationError,
)
from corpscout_translation_service.service import TranslationService


LOGGER = logging.getLogger(__name__)
DEFAULT_NATS_SUBJECT = "brreg.translation.translate"
DEFAULT_NATS_QUEUE = "brreg-translation"


class NatsMessage(Protocol):
    data: bytes

    async def respond(self, payload: bytes) -> None: ...


class BrregTranslationService(Protocol):
    async def translate_brreg_records(self, request: BrregTranslateRequest) -> BrregTranslateResponse: ...


async def handle_brreg_translation_message(
    message: NatsMessage,
    service: BrregTranslationService,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = BrregTranslateRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        await _respond(message, _failed_response("invalid_nats_payload", "Invalid BRREG translation request.", exc))
        return

    try:
        response = await service.translate_brreg_records(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("BRREG translation NATS handler failed")
        response = _failed_response("translation_worker_error", "BRREG translation worker failed.", exc, request)

    await _respond(message, response)


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service = TranslationService()
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    subject = os.getenv("TRANSLATION_NATS_SUBJECT", os.getenv("NATS_SUBJECT", DEFAULT_NATS_SUBJECT))
    queue = os.getenv("TRANSLATION_NATS_QUEUE", os.getenv("NATS_QUEUE", DEFAULT_NATS_QUEUE))
    max_concurrent = _positive_int_from_env("TRANSLATION_WORKER_MAX_CONCURRENT_REQUESTS", 1)
    semaphore = asyncio.Semaphore(max_concurrent)

    nc = await nats.connect(nats_url)
    LOGGER.info(
        "translation-service NATS worker connected subject=%s queue=%s max_concurrent=%s",
        subject,
        queue,
        max_concurrent,
    )

    async def callback(message: Any) -> None:
        async with semaphore:
            await handle_brreg_translation_message(message, service)

    try:
        await nc.subscribe(subject, queue=queue, cb=callback)
        while True:
            await asyncio.sleep(3600)
    finally:
        await nc.drain()


def _failed_response(
    code: str,
    message: str,
    exc: Exception,
    request: BrregTranslateRequest | None = None,
) -> BrregTranslateResponse:
    error = TranslationError(
        code=code,
        message=message,
        category="translation_service",
        retry_strategy="retry_with_backoff",
        detail={"error": str(exc)},
    )
    records = request.records if request is not None else []
    results = [
        BrregRecordTranslationResult(
            record_id=record.record_id,
            organization_number=record.organization_number,
            status="failed",
            missing_terms=[],
            error=error,
            duration_ms=0,
        )
        for record in records
    ]
    if not results:
        results = [
            BrregRecordTranslationResult(
                record_id="unknown",
                organization_number="unknown",
                status="failed",
                missing_terms=[],
                error=error,
                duration_ms=0,
            )
        ]
    return BrregTranslateResponse(
        status="failed",
        provider=request.llm.provider if request is not None else "unknown",
        model=(request.llm.model or "") if request is not None else "unknown",
        prompt_version=request.prompt_version if request is not None else "unknown",
        records_seen=len(records),
        records_completed=0,
        records_failed=len(results),
        records_skipped=0,
        duration_ms=0,
        results=results,
    )


async def _respond(message: NatsMessage, response: BrregTranslateResponse) -> None:
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
