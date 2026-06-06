from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from typing import Any

from corpscout_translation_service.brreg import (
    TranslationItem,
    build_translation_payload,
    extract_translation_items,
    to_llm_item,
    translation_item_id,
)
from corpscout_translation_service.llm import (
    LLMClient,
    OpenAICompatibleLLMClient,
    default_model,
    default_provider,
    provider_model,
)
from corpscout_translation_service.mocking import MockTranslationController, mock_enabled_from_env
from corpscout_translation_service.models import (
    BrregRecord,
    BrregRecordTranslationResult,
    BrregTranslateRequest,
    BrregTranslateResponse,
    LLMSelection,
    LLMTranslationItem,
    LLMTranslationRequest,
    LLMTermTranslation,
    LLMTranslateResponse,
    TermTranslationFailureResult,
    TermTranslationRequest,
    TermTranslationRequestTerm,
    TermTranslationResponse,
    TermTranslationResultItem,
    TranslationError,
)


logger = logging.getLogger(__name__)


class TranslationService:
    def __init__(
        self,
        *,
        llm_client: LLMClient | None = None,
        max_llm_items_per_request: int = 50,
        mock_controller: MockTranslationController | None = None,
    ) -> None:
        if max_llm_items_per_request <= 0:
            raise ValueError("max_llm_items_per_request must be positive")
        self._llm_client = llm_client or OpenAICompatibleLLMClient.from_env()
        self._max_llm_items_per_request = max_llm_items_per_request
        self._mock_controller = mock_controller or MockTranslationController.from_env()
        self.mock_enabled = mock_enabled_from_env()

    def reset_mock_state(self) -> None:
        self._mock_controller.reset()

    def mock_state(self) -> dict[str, object]:
        return self._mock_controller.state()

    async def aclose(self) -> None:
        close = getattr(self._llm_client, "aclose", None)
        if close is not None:
            await close()

    async def translate_brreg_records(self, request: BrregTranslateRequest) -> BrregTranslateResponse:
        started = time.monotonic()
        llm = _effective_llm_selection(request.llm)
        items_by_record_id = {
            record.record_id: extract_translation_items(record.raw_payload)
            for record in request.records
        }

        try:
            translated_by_id = await self._translate_unique_items(
                items_by_record_id.values(),
                llm=llm,
                prompt_version=request.prompt_version,
                source_lang=request.source_lang,
                target_lang=request.target_lang,
                max_retries=request.max_retries,
            )
            results = [
                self._build_record_result(
                    record=record,
                    items=items_by_record_id[record.record_id],
                    translated_by_id=translated_by_id,
                    model=llm.model or default_model(),
                    prompt_version=request.prompt_version,
                    source_lang=request.source_lang,
                    target_lang=request.target_lang,
                    started=started,
                )
                for record in request.records
            ]
        except Exception as exc:
            logger.exception("BRREG translation request failed")
            results = [
                _failed_result(
                    record=record,
                    code="llm_request_failed",
                    message="LLM request failed",
                    detail={"error": str(exc), "error_type": type(exc).__name__},
                    started=started,
                )
                for record in request.records
            ]

        return _response(
            provider=llm.provider,
            model=llm.model or default_model(),
            prompt_version=request.prompt_version,
            records_seen=len(request.records),
            results=results,
            started=started,
        )

    async def translate_terms(self, request: LLMTranslationRequest) -> LLMTranslateResponse:
        started = time.monotonic()
        provider = request.provider if request.provider != "default" else default_provider()
        model = request.model if request.model and request.model != "default" else provider_model(provider)
        llm_request = request.model_copy(update={"provider": provider, "model": model})

        if provider == "mock":
            return self._mock_controller.translate_terms_response(llm_request)

        try:
            translated_by_id = await self._translate_terms_with_missing_retries(llm_request)
        except Exception as exc:
            logger.exception("Term translation request failed")
            return LLMTranslateResponse(
                status="failed",
                provider=provider,
                model=model,
                prompt_version=request.prompt_version,
                items_seen=len(request.items),
                items_completed=0,
                items_failed=len(request.items),
                translations=[],
                missing_ids=[item.id for item in request.items],
                error=TranslationError(
                    code="llm_request_failed",
                    message="LLM request failed",
                    detail={"error": str(exc), "error_type": type(exc).__name__},
                ),
                duration_ms=_elapsed_ms(started),
            )

        translations = [
            LLMTermTranslation(id=item.id, translation=translated_by_id[item.id])
            for item in request.items
            if translated_by_id.get(item.id)
        ]
        translated_ids = {item.id for item in translations}
        missing_ids = [item.id for item in request.items if item.id not in translated_ids]
        status = "succeeded" if not missing_ids else "partial" if translations else "failed"
        error = None
        if missing_ids:
            error = TranslationError(
                code="missing_translations",
                message="LLM response missed translation terms after retries",
                category="invalid_llm_output",
                retry_strategy="change_model_or_prompt",
                detail={
                    "missing_count": len(missing_ids),
                    "missing_ids_sample": missing_ids[:20],
                    "max_retries": request.max_retries,
                },
            )
        return LLMTranslateResponse(
            status=status,
            provider=provider,
            model=model,
            prompt_version=request.prompt_version,
            items_seen=len(request.items),
            items_completed=len(translations),
            items_failed=len(missing_ids),
            translations=translations,
            missing_ids=missing_ids,
            error=error,
            duration_ms=_elapsed_ms(started),
        )

    async def translate_source_terms(self, request: TermTranslationRequest) -> TermTranslationResponse:
        provider = request.provider if request.provider != "default" else default_provider()
        model = request.model if request.model and request.model != "default" else provider_model(provider)
        terms_by_key = {term.term_key: term for term in request.terms}
        local_id_by_term_key = {
            term.term_key: _local_translation_item_id(index)
            for index, term in enumerate(request.terms)
        }
        term_key_by_local_id = {local_id: term_key for term_key, local_id in local_id_by_term_key.items()}
        llm_request = LLMTranslationRequest(
            provider=provider,
            model=model,
            prompt_version=request.prompt_version,
            source_lang=request.source_lang,
            target_lang=request.target_lang,
            items=[
                LLMTranslationItem(
                    id=local_id_by_term_key[term.term_key],
                    category=f"{request.source}_term",
                    text=term.source_text,
                )
                for term in request.terms
            ],
        )

        try:
            llm_response = await self.translate_terms(llm_request)
        except Exception as exc:
            logger.exception("Source term translation request failed", extra={"source": request.source})
            return TermTranslationResponse(
                request_id=request.request_id,
                source=request.source,
                source_lang=request.source_lang,
                target_lang=request.target_lang,
                provider=provider,
                model=model,
                prompt_version=request.prompt_version,
                failures=[
                    _term_failure(term, error_code="llm_request_failed", error=str(exc))
                    for term in request.terms
                ],
            )

        translated_by_key = {
            term_key_by_local_id[translation.id]: translation.translation
            for translation in llm_response.translations
            if translation.id in term_key_by_local_id
        }
        results = [
            TermTranslationResultItem(
                term_key=term.term_key,
                source_text=term.source_text,
                source_text_normalized=term.source_text_normalized,
                translated_text=translated_by_key[term.term_key],
            )
            for term in request.terms
            if term.term_key in translated_by_key
        ]

        if llm_response.error is not None and llm_response.error.code == "llm_request_failed":
            failures = [
                _term_failure(term, error_code="llm_request_failed", error=_llm_error_text(llm_response.error))
                for term in request.terms
                if term.term_key not in translated_by_key
            ]
        else:
            failures = [
                _term_failure(terms_by_key[term_key], error_code="missing_term_translation")
                for term_key in _term_keys_from_local_ids(llm_response.missing_ids, term_key_by_local_id)
                if term_key in terms_by_key
            ]

        return TermTranslationResponse(
            request_id=request.request_id,
            source=request.source,
            source_lang=request.source_lang,
            target_lang=request.target_lang,
            provider=llm_response.provider,
            model=llm_response.model,
            prompt_version=llm_response.prompt_version,
            results=results,
            failures=failures,
        )

    async def translate_brreg_terms(self, request: TermTranslationRequest) -> TermTranslationResponse:
        return await self.translate_source_terms(request)

    async def _translate_unique_items(
        self,
        item_groups: Iterable[list[TranslationItem]],
        *,
        llm: LLMSelection,
        prompt_version: str,
        source_lang: str,
        target_lang: str,
        max_retries: int,
    ) -> dict[str, str]:
        llm_items = _unique_llm_items(item for items in item_groups for item in items)
        translated_by_id: dict[str, str] = {}
        pending = llm_items
        for attempt in range(max_retries + 1):
            if not pending:
                break
            chunks = list(_chunks(pending, self._max_llm_items_per_request))
            for chunk_index, chunk in enumerate(chunks, start=1):
                logger.info(
                    "Translating BRREG terms with LLM",
                    extra={
                        "attempt": attempt + 1,
                        "chunk_index": chunk_index,
                        "chunk_count": len(chunks),
                        "chunk_items": len(chunk),
                        "pending_items": len(pending),
                        "model": llm.model,
                        "provider": llm.provider,
                    },
                )
                request = LLMTranslationRequest(
                    provider=llm.provider,
                    model=llm.model or default_model(),
                    base_url=llm.base_url,
                    api_key=llm.api_key,
                    prompt_version=prompt_version,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    items=chunk,
                )
                translated_by_id.update(await self._translate_terms(request))
            pending = [item for item in llm_items if item.id not in translated_by_id]
            if pending:
                logger.warning(
                    "LLM response missed translation terms; retrying",
                    extra={"missing_terms": len(pending), "attempt": attempt + 1},
                )
        return translated_by_id

    async def _translate_terms(self, request: LLMTranslationRequest) -> dict[str, str]:
        if request.provider == "mock":
            response = self._mock_controller.translate_terms_response(request)
            if response.status == "failed":
                error = response.error
                message = error.message if error is not None else "mock translation failed"
                raise RuntimeError(message)
            return {item.id: item.translation for item in response.translations}
        return await self._llm_client.translate_terms(request)

    async def _translate_terms_with_missing_retries(self, request: LLMTranslationRequest) -> dict[str, str]:
        translated_by_id: dict[str, str] = {}
        pending = request.items
        for attempt in range(request.max_retries + 1):
            if not pending:
                break
            chunks = list(_chunks(pending, self._max_llm_items_per_request))
            for chunk_index, chunk in enumerate(chunks, start=1):
                logger.info(
                    "Translating term batch with LLM",
                    extra={
                        "attempt": attempt + 1,
                        "chunk_index": chunk_index,
                        "chunk_count": len(chunks),
                        "chunk_items": len(chunk),
                        "pending_items": len(pending),
                        "model": request.model,
                        "provider": request.provider,
                    },
                )
                chunk_request = request.model_copy(update={"items": chunk})
                translated_by_id.update(await self._translate_terms(chunk_request))
            pending = [item for item in request.items if item.id not in translated_by_id]
            if pending and attempt < request.max_retries:
                logger.warning(
                    "LLM response missed translation terms; retrying",
                    extra={
                        "missing_terms": len(pending),
                        "attempt": attempt + 1,
                        "next_attempt": attempt + 2,
                        "max_retries": request.max_retries,
                    },
                )
            elif pending:
                logger.error(
                    "LLM response missed translation terms after retries",
                    extra={
                        "missing_terms": len(pending),
                        "attempts": attempt + 1,
                        "max_retries": request.max_retries,
                        "missing_ids_sample": [item.id for item in pending[:20]],
                    },
                )
        return translated_by_id

    def _build_record_result(
        self,
        *,
        record: BrregRecord,
        items: list[TranslationItem],
        translated_by_id: dict[str, str],
        model: str,
        prompt_version: str,
        source_lang: str,
        target_lang: str,
        started: float,
    ) -> BrregRecordTranslationResult:
        if not items:
            return BrregRecordTranslationResult(
                record_id=record.record_id,
                organization_number=record.organization_number,
                status="skipped",
                translated_payload=build_translation_payload(
                    raw_payload=record.raw_payload,
                    items=[],
                    translated_by_id={},
                    model=model,
                    prompt_version=prompt_version,
                    source_lang=source_lang,
                    target_lang=target_lang,
                ),
                duration_ms=_elapsed_ms(started),
            )

        missing_terms = [translation_item_id(item) for item in items if translation_item_id(item) not in translated_by_id]
        if missing_terms:
            logger.error(
                "BRREG record translation failed because LLM omitted terms",
                extra={"record_id": record.record_id, "organization_number": record.organization_number},
            )
            return _failed_result(
                record=record,
                code="missing_translations",
                message="LLM response did not include every required translation term",
                detail={"missing_terms": missing_terms},
                missing_terms=missing_terms,
                started=started,
            )

        return BrregRecordTranslationResult(
            record_id=record.record_id,
            organization_number=record.organization_number,
            status="succeeded",
            translated_payload=build_translation_payload(
                raw_payload=record.raw_payload,
                items=items,
                translated_by_id=translated_by_id,
                model=model,
                prompt_version=prompt_version,
                source_lang=source_lang,
                target_lang=target_lang,
            ),
            duration_ms=_elapsed_ms(started),
        )


def _unique_llm_items(items: Iterable[TranslationItem]) -> list[LLMTranslationItem]:
    seen: set[str] = set()
    unique: list[LLMTranslationItem] = []
    for item in sorted(items, key=lambda value: (value.category, value.text.strip().lower())):
        llm_item = to_llm_item(item)
        if llm_item.id in seen:
            continue
        seen.add(llm_item.id)
        unique.append(llm_item)
    return unique


def _chunks(items: list[LLMTranslationItem], size: int) -> Iterable[list[LLMTranslationItem]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _local_translation_item_id(index: int) -> str:
    return f"t{index + 1:03d}"


def _term_keys_from_local_ids(local_ids: Iterable[str], term_key_by_local_id: dict[str, str]) -> list[str]:
    return [
        term_key_by_local_id[local_id]
        for local_id in local_ids
        if local_id in term_key_by_local_id
    ]


def _failed_result(
    *,
    record: BrregRecord,
    code: str,
    message: str,
    detail: dict[str, Any],
    started: float,
    missing_terms: list[str] | None = None,
) -> BrregRecordTranslationResult:
    return BrregRecordTranslationResult(
        record_id=record.record_id,
        organization_number=record.organization_number,
        status="failed",
        missing_terms=missing_terms or [],
        error=TranslationError(code=code, message=message, detail=detail),
        duration_ms=_elapsed_ms(started),
    )


def _term_failure(
    term: TermTranslationRequestTerm,
    *,
    error_code: str,
    error: str | None = None,
) -> TermTranslationFailureResult:
    return TermTranslationFailureResult(
        term_key=term.term_key,
        source_text=term.source_text,
        source_text_normalized=term.source_text_normalized,
        status="failed_retryable",
        error_code=error_code,
        error=error,
    )


def _llm_error_text(error: TranslationError) -> str:
    detail_error = error.detail.get("error")
    if isinstance(detail_error, str) and detail_error:
        return detail_error
    return error.message


def _response(
    *,
    provider: str,
    model: str,
    prompt_version: str,
    records_seen: int,
    results: list[BrregRecordTranslationResult],
    started: float,
) -> BrregTranslateResponse:
    completed = sum(1 for result in results if result.status == "succeeded")
    failed = sum(1 for result in results if result.status == "failed")
    skipped = sum(1 for result in results if result.status == "skipped")
    if failed == 0:
        status = "succeeded"
    elif completed == 0 and skipped == 0:
        status = "failed"
    else:
        status = "partial"
    return BrregTranslateResponse(
        status=status,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        records_seen=records_seen,
        records_completed=completed,
        records_failed=failed,
        records_skipped=skipped,
        duration_ms=_elapsed_ms(started),
        results=results,
    )


def _effective_llm_selection(selection: LLMSelection) -> LLMSelection:
    provider = selection.provider if selection.provider != "default" else default_provider()
    model = selection.model or provider_model(provider)
    return selection.model_copy(update={"provider": provider, "model": model})


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)
