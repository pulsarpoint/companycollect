from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import logging
import os
import re
import time
from typing import Any, Protocol

import nats
from nats.js.errors import FetchTimeoutError
from pydantic import ValidationError

from corpscout_translation_service.models import (
    BrregRecordTranslationResult,
    BrregTranslateRequest,
    BrregTranslateResponse,
    JetStreamTranslationFailureItem,
    JetStreamTranslationJob,
    JetStreamTranslationResult,
    JetStreamTranslationResultItem,
    TermTranslationFailureResult,
    TermTranslationRequest,
    TermTranslationRequestTerm,
    TermTranslationResponse,
    TranslationError,
)
from corpscout_translation_service.service import TranslationService


LOGGER = logging.getLogger(__name__)
DEFAULT_NATS_SUBJECT = "brreg.translation.translate"
DEFAULT_NATS_QUEUE = "brreg-translation"
TERM_REQUEST_SUBJECT = "brreg.translation.terms.request"
TERM_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")
JETSTREAM_JOB_SUBJECT = "source.translation.jobs"
JETSTREAM_RESULT_SUBJECT = "source.translation.results"
JETSTREAM_STREAM = "SOURCE_TRANSLATION"
JETSTREAM_DURABLE = "translation-service"


class NatsMessage(Protocol):
    data: bytes

    async def respond(self, payload: bytes) -> None: ...


class BrregRecordTranslationService(Protocol):
    async def translate_brreg_records(self, request: BrregTranslateRequest) -> BrregTranslateResponse: ...


class SourceTermTranslationService(Protocol):
    async def translate_source_terms(self, request: TermTranslationRequest) -> TermTranslationResponse: ...


class ResultPublisher(Protocol):
    async def publish_result(self, result: JetStreamTranslationResult) -> None: ...


async def handle_brreg_translation_message(
    message: NatsMessage,
    service: BrregRecordTranslationService,
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


async def handle_source_term_translation_message(
    message: NatsMessage,
    service: SourceTermTranslationService,
) -> None:
    payload: Any | None = None
    try:
        payload = json.loads(message.data.decode("utf-8"))
        request = TermTranslationRequest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        response = _failed_term_response_from_payload(payload, exc)
        if response is not None:
            await _respond_if_supported(message, response)
        LOGGER.exception("Invalid source term translation NATS payload")
        return

    try:
        response = await service.translate_source_terms(request)
    except Exception as exc:  # noqa: BLE001 - boundary handler must return structured failures.
        LOGGER.exception("Source term translation NATS handler failed")
        response = _failed_term_response(request, "translation_worker_error", str(exc))

    await _respond_if_supported(message, response)


handle_brreg_term_translation_message = handle_source_term_translation_message


async def handle_jetstream_translation_job(
    message: Any,
    service: SourceTermTranslationService,
    publisher: ResultPublisher,
) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
        job = JetStreamTranslationJob.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError):
        await _ack_if_supported(message)
        LOGGER.exception("Invalid JetStream translation job payload")
        return

    started = time.monotonic()
    await message.ack()
    request = TermTranslationRequest(
        request_id=job.job_id,
        source=job.source,
        source_lang=job.source_lang,
        target_lang=job.target_lang,
        provider=job.provider,
        model=job.model,
        prompt_version=job.prompt_version,
        terms=[
            TermTranslationRequestTerm(
                term_key=term.term_key,
                source_text=term.source_text,
                source_text_normalized=term.source_text_normalized,
            )
            for term in job.terms
        ],
    )

    try:
        response = await service.translate_source_terms(request)
        result = JetStreamTranslationResult(
            job_id=job.job_id,
            batch_id=job.batch_id,
            source=job.source,
            status=_jetstream_status_from_term_response(response),
            provider=response.provider,
            model=response.model,
            prompt_version=response.prompt_version,
            company_ids=job.company_ids,
            duration_ms=_elapsed_ms(started),
            results=[
                JetStreamTranslationResultItem(
                    term_key=item.term_key,
                    source_text=item.source_text,
                    source_text_normalized=item.source_text_normalized,
                    translated_text=item.translated_text,
                    status=item.status,
                )
                for item in response.results
            ],
            failures=[
                JetStreamTranslationFailureItem(
                    term_key=item.term_key,
                    source_text=item.source_text,
                    source_text_normalized=item.source_text_normalized,
                    status=item.status,
                    error_code=item.error_code,
                    error=item.error,
                )
                for item in response.failures
            ],
        )
    except Exception as exc:  # noqa: BLE001 - boundary handler must publish a structured failure result.
        LOGGER.exception("JetStream translation job failed")
        result = JetStreamTranslationResult(
            job_id=job.job_id,
            batch_id=job.batch_id,
            source=job.source,
            status="failed",
            provider=job.provider,
            model=job.model,
            prompt_version=job.prompt_version,
            company_ids=job.company_ids,
            duration_ms=_elapsed_ms(started),
            failures=[
                JetStreamTranslationFailureItem(
                    term_key=term.term_key,
                    source_text=term.source_text,
                    source_text_normalized=term.source_text_normalized,
                    status="failed_retryable",
                    error_code="translation_worker_error",
                    error=str(exc),
                )
                for term in job.terms
            ],
        )

    await publisher.publish_result(result)


class JetStreamResultPublisher:
    def __init__(self, js: Any) -> None:
        self._js = js

    async def publish_result(self, result: JetStreamTranslationResult) -> None:
        await self._js.publish(
            JETSTREAM_RESULT_SUBJECT,
            result.model_dump_json(exclude_none=True).encode("utf-8"),
        )


async def run_jetstream_translation_loop(
    pull_subscription: Any,
    service: SourceTermTranslationService,
    publisher: ResultPublisher,
    *,
    semaphore: asyncio.Semaphore,
    fetch_timeout_seconds: float = 1.0,
) -> None:
    while True:
        try:
            messages = await pull_subscription.fetch(batch=1, timeout=fetch_timeout_seconds)
        except (FetchTimeoutError, nats.errors.TimeoutError, TimeoutError):
            continue
        for message in messages:
            async with semaphore:
                await handle_jetstream_translation_job(message, service, publisher)


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service = TranslationService()
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    subject = os.getenv("TRANSLATION_NATS_SUBJECT", os.getenv("NATS_SUBJECT", DEFAULT_NATS_SUBJECT))
    queue = os.getenv("TRANSLATION_NATS_QUEUE", os.getenv("NATS_QUEUE", DEFAULT_NATS_QUEUE))
    max_concurrent = _positive_int_from_env("TRANSLATION_WORKER_MAX_CONCURRENT_REQUESTS", 1)
    semaphore = asyncio.Semaphore(max_concurrent)

    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    await _ensure_jetstream_stream(js)
    pull_subscription = await js.pull_subscribe(
        JETSTREAM_JOB_SUBJECT,
        durable=JETSTREAM_DURABLE,
        stream=JETSTREAM_STREAM,
    )
    result_publisher = JetStreamResultPublisher(js)
    LOGGER.info(
        "translation-service NATS worker connected subject=%s queue=%s max_concurrent=%s jetstream_stream=%s",
        subject,
        queue,
        max_concurrent,
        JETSTREAM_STREAM,
    )

    async def callback(message: Any) -> None:
        async with semaphore:
            await handle_brreg_translation_message(message, service)

    async def term_callback(message: Any) -> None:
        async with semaphore:
            await handle_source_term_translation_message(message, service)

    jetstream_task: asyncio.Task[None] | None = None
    try:
        jetstream_task = asyncio.create_task(
            run_jetstream_translation_loop(
                pull_subscription,
                service,
                result_publisher,
                semaphore=semaphore,
            )
        )
        await nc.subscribe(subject, queue=queue, cb=callback)
        await nc.subscribe(TERM_REQUEST_SUBJECT, queue=queue, cb=term_callback)
        while True:
            await asyncio.sleep(3600)
    finally:
        if jetstream_task is not None:
            jetstream_task.cancel()
            with suppress(asyncio.CancelledError):
                await jetstream_task
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


async def _respond_if_supported(message: NatsMessage, response: TermTranslationResponse) -> None:
    respond = getattr(message, "respond", None)
    if respond is None:
        return
    payload = response.model_dump_json(exclude_none=True).encode("utf-8")
    await respond(payload)


async def _ack_if_supported(message: Any) -> None:
    ack = getattr(message, "ack", None)
    if ack is None:
        return
    await ack()


async def _ensure_jetstream_stream(js: Any) -> None:
    try:
        await js.add_stream(
            name=JETSTREAM_STREAM,
            subjects=[JETSTREAM_JOB_SUBJECT, JETSTREAM_RESULT_SUBJECT],
        )
    except Exception as exc:
        if _is_stream_already_exists_error(exc):
            return
        raise


def _is_stream_already_exists_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "stream name already in use" in message or "stream already exists" in message


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


def _jetstream_status_from_term_response(response: TermTranslationResponse) -> str:
    if response.failures and not response.results:
        return "failed"
    if response.failures:
        return "partial"
    return "succeeded"


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


def _positive_int_from_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default
