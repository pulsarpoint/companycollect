from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Protocol

import nats
from pydantic import ValidationError

from corpscout_translation_service.models import (
    BrregRecordTranslationResult,
    BrregTranslateRequest,
    BrregTranslateResponse,
    TermTranslationFailureResult,
    TermTranslationRequest,
    TermTranslationResponse,
    TranslationError,
)
from corpscout_translation_service.service import TranslationService


LOGGER = logging.getLogger(__name__)
DEFAULT_NATS_SUBJECT = "brreg.translation.translate"
DEFAULT_NATS_QUEUE = "brreg-translation"
TERM_REQUEST_SUBJECT = "brreg.translation.terms.request"
TERM_RESULT_SUBJECT = "brreg.translation.terms.result"
TERM_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class NatsMessage(Protocol):
    data: bytes

    async def respond(self, payload: bytes) -> None: ...


class NatsPublisher(Protocol):
    async def publish(self, subject: str, payload: bytes) -> None: ...


class BrregTranslationService(Protocol):
    async def translate_brreg_records(self, request: BrregTranslateRequest) -> BrregTranslateResponse: ...

    async def translate_brreg_terms(self, request: TermTranslationRequest) -> TermTranslationResponse: ...


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


async def handle_brreg_term_translation_message(
    message: NatsMessage,
    service: BrregTranslationService,
    publisher: NatsPublisher,
) -> None:
    payload: Any | None = None
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = TermTranslationRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        response = _failed_term_response_from_payload(payload, exc)
        if response is not None:
            await _publish_terms_response(publisher, response)
        LOGGER.exception("Invalid BRREG term translation NATS payload")
        await _ack_if_supported(message)
        return

    try:
        response = await service.translate_brreg_terms(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("BRREG term translation NATS handler failed")
        response = _failed_term_response(request, "translation_worker_error", str(exc))

    await _publish_terms_response(publisher, response)
    await _respond_if_supported(message, response)
    await _ack_if_supported(message)


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

    async def term_callback(message: Any) -> None:
        async with semaphore:
            await handle_brreg_term_translation_message(message, service, nc)

    try:
        await nc.subscribe(subject, queue=queue, cb=callback)
        await nc.subscribe(TERM_REQUEST_SUBJECT, queue=queue, cb=term_callback)
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


async def _publish_terms_response(publisher: NatsPublisher, response: TermTranslationResponse) -> None:
    payload = response.model_dump_json(exclude_none=True).encode("utf-8")
    await publisher.publish(TERM_RESULT_SUBJECT, payload)


async def _respond_if_supported(message: NatsMessage, response: TermTranslationResponse) -> None:
    respond = getattr(message, "respond", None)
    if respond is None:
        return
    payload = response.model_dump_json(exclude_none=True).encode("utf-8")
    await respond(payload)


async def _ack_if_supported(message: NatsMessage) -> None:
    ack = getattr(message, "ack", None)
    if ack is not None:
        await ack()


def _failed_term_response(
    request: TermTranslationRequest,
    code: str,
    error: str,
) -> TermTranslationResponse:
    return TermTranslationResponse(
        request_id=request.request_id,
        source=request.source,
        source_lang=request.source_lang,
        target_lang=request.target_lang,
        provider=request.provider,
        model=request.model,
        prompt_version=request.prompt_version,
        failures=[
            TermTranslationFailureResult(
                term_key=term.term_key,
                source_text=term.source_text,
                source_text_normalized=term.source_text_normalized,
                status="failed_retryable",
                error_code=code,
                error=error,
            )
            for term in request.terms
        ],
    )


def _failed_term_response_from_payload(
    payload: Any,
    exc: Exception,
) -> TermTranslationResponse | None:
    if not isinstance(payload, dict) or not payload.get("request_id"):
        return None
    failures: list[TermTranslationFailureResult] = []
    seen_term_keys: set[str] = set()
    for term in payload.get("terms") or []:
        if (
            not isinstance(term, dict)
            or not _valid_term_key(term.get("term_key"))
            or not term.get("source_text")
            or not term.get("source_text_normalized")
        ):
            continue
        term_key = str(term.get("term_key"))
        if term_key in seen_term_keys:
            continue
        seen_term_keys.add(term_key)
        failures.append(
            TermTranslationFailureResult(
                term_key=term_key,
                source_text=str(term.get("source_text")),
                source_text_normalized=str(term.get("source_text_normalized")),
                status="failed_retryable",
                error_code="invalid_nats_payload",
                error=str(exc),
            )
        )
    if not failures:
        return None

    return TermTranslationResponse(
        request_id=str(payload.get("request_id")),
        source=str(payload.get("source") or "brreg"),
        source_lang=str(payload.get("source_lang") or "no"),
        target_lang=str(payload.get("target_lang") or "en"),
        provider=str(payload.get("provider") or "default"),
        model=payload.get("model") if isinstance(payload.get("model"), str) else None,
        prompt_version=str(payload.get("prompt_version") or "v1"),
        failures=failures,
    )


def _valid_term_key(value: Any) -> bool:
    return isinstance(value, str) and TERM_KEY_PATTERN.fullmatch(value) is not None


def _positive_int_from_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default
