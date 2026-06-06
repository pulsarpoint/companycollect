from __future__ import annotations

import pytest
from pydantic import ValidationError

from corpscout_translation_service.models import (
    TermTranslationRequest,
    TermTranslationRequestTerm,
)
from corpscout_translation_service.service import TranslationService

from tests.fakes import FakeLLMClient


TERM_KEY_1 = "6b79e9d3d6b2cfb0c065d83384c1028947fb5f89af7938f4e176122bdd26db72"
TERM_KEY_2 = "13023b7618e56ed2955cfe4929b29dce8df343a9096784613721493d95e1663c"


@pytest.mark.asyncio
async def test_source_term_translation_maps_terms_through_llm_request() -> None:
    llm_client = FakeLLMClient()
    service = TranslationService(llm_client=llm_client, max_llm_items_per_request=50)

    response = await service.translate_source_terms(
        TermTranslationRequest(
            request_id="request-1",
            source="ariregister",
            source_lang="et",
            provider="fake",
            model="fake-fast",
            terms=[
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_1,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                )
            ],
        )
    )

    assert response.request_id == "request-1"
    assert response.source == "ariregister"
    assert response.source_lang == "et"
    assert response.target_lang == "en"
    assert response.provider == "fake"
    assert response.model == "fake-fast"
    assert response.prompt_version == "v1"
    assert response.failures == []
    assert [result.model_dump(mode="json") for result in response.results] == [
        {
            "term_key": TERM_KEY_1,
            "source_text": "Aksjeselskap",
            "source_text_normalized": "aksjeselskap",
            "translated_text": "Aksjeselskap EN",
            "status": "succeeded",
        }
    ]
    assert len(llm_client.calls) == 1
    llm_request = llm_client.calls[0]
    assert llm_request.provider == "fake"
    assert llm_request.model == "fake-fast"
    assert llm_request.prompt_version == "v1"
    assert llm_request.source_lang == "et"
    assert llm_request.target_lang == "en"
    assert [item.model_dump(mode="json") for item in llm_request.items] == [
        {"id": TERM_KEY_1, "category": "ariregister_term", "text": "Aksjeselskap"}
    ]


@pytest.mark.asyncio
async def test_source_term_translation_maps_missing_llm_terms_to_retryable_failures() -> None:
    service = TranslationService(
        llm_client=FakeLLMClient(always_missing_ids={TERM_KEY_2}),
        max_llm_items_per_request=50,
    )

    response = await service.translate_source_terms(
        TermTranslationRequest(
            request_id="request-2",
            provider="fake",
            model="fake-fast",
            terms=[
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_1,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                ),
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_2,
                    source_text="Ukjent verdi",
                    source_text_normalized="ukjent verdi",
                ),
            ],
        )
    )

    assert [result.term_key for result in response.results] == [TERM_KEY_1]
    assert response.results[0].translated_text == "Aksjeselskap EN"
    assert [failure.model_dump(mode="json", exclude_none=True) for failure in response.failures] == [
        {
            "term_key": TERM_KEY_2,
            "source_text": "Ukjent verdi",
            "source_text_normalized": "ukjent verdi",
            "status": "failed_retryable",
            "error_code": "missing_term_translation",
        }
    ]


@pytest.mark.asyncio
async def test_source_term_translation_returns_retryable_failures_for_total_llm_exception() -> None:
    service = TranslationService(
        llm_client=FakeLLMClient(fail_with=RuntimeError("request timed out")),
        max_llm_items_per_request=50,
    )

    response = await service.translate_source_terms(
        TermTranslationRequest(
            request_id="request-3",
            provider="fake",
            model="fake-fast",
            terms=[
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_1,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                )
            ],
        )
    )

    assert response.results == []
    assert [failure.model_dump(mode="json", exclude_none=True) for failure in response.failures] == [
        {
            "term_key": TERM_KEY_1,
            "source_text": "Aksjeselskap",
            "source_text_normalized": "aksjeselskap",
            "status": "failed_retryable",
            "error_code": "llm_request_failed",
            "error": "request timed out",
        }
    ]


@pytest.mark.asyncio
async def test_brreg_term_translation_method_remains_legacy_alias() -> None:
    service = TranslationService(llm_client=FakeLLMClient(), max_llm_items_per_request=50)

    response = await service.translate_brreg_terms(
        TermTranslationRequest(
            request_id="request-legacy",
            provider="fake",
            model="fake-fast",
            terms=[
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_1,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                )
            ],
        )
    )

    assert response.request_id == "request-legacy"
    assert response.source == "brreg"
    assert response.results[0].translated_text == "Aksjeselskap EN"


def test_brreg_term_translation_rejects_non_sha256_term_key() -> None:
    with pytest.raises(ValidationError):
        TermTranslationRequestTerm(
            term_key="term-1",
            source_text="Aksjeselskap",
            source_text_normalized="aksjeselskap",
        )


def test_brreg_term_translation_rejects_duplicate_term_keys() -> None:
    with pytest.raises(ValidationError):
        TermTranslationRequest(
            request_id="request-duplicate",
            provider="fake",
            model="fake-fast",
            terms=[
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_1,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                ),
                TermTranslationRequestTerm(
                    term_key=TERM_KEY_1,
                    source_text="Annen tekst",
                    source_text_normalized="annen tekst",
                ),
            ],
        )
