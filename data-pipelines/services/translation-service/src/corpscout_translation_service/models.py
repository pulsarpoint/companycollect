from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


RecordStatus = Literal["succeeded", "failed", "skipped"]
BatchStatus = Literal["succeeded", "partial", "failed"]
TermResultStatus = Literal["succeeded"]
TermFailureStatus = Literal["failed_retryable", "failed_terminal"]
TERM_KEY_PATTERN = r"^[0-9a-f]{64}$"


class LLMSelection(BaseModel):
    provider: str = Field(default="default", min_length=1)
    model: str | None = Field(default=None, min_length=1)
    base_url: str | None = Field(default=None, min_length=1)
    api_key: str | None = Field(default=None)


class BrregRecord(BaseModel):
    record_id: str = Field(min_length=1)
    organization_number: str = Field(min_length=1)
    raw_payload: dict[str, Any]


class BrregTranslateRequest(BaseModel):
    records: list[BrregRecord] = Field(min_length=1, max_length=1000)
    llm: LLMSelection = Field(default_factory=LLMSelection)
    prompt_version: str = Field(default="v1", min_length=1)
    source_lang: str = Field(default="no", min_length=2)
    target_lang: str = Field(default="en", min_length=2)
    max_retries: int = Field(default=3, ge=0, le=5)


class LLMTranslationItem(BaseModel):
    id: str
    category: str
    text: str


class LLMTranslationRequest(BaseModel):
    provider: str
    model: str
    base_url: str | None = Field(default=None, min_length=1)
    api_key: str | None = Field(default=None)
    prompt_version: str
    source_lang: str
    target_lang: str
    items: list[LLMTranslationItem] = Field(min_length=1)
    max_retries: int = Field(default=3, ge=0, le=5)


class LLMTermTranslation(BaseModel):
    id: str
    translation: str


class TranslationError(BaseModel):
    code: str
    message: str
    category: str | None = None
    retry_strategy: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class LLMTranslateResponse(BaseModel):
    schema_version: str = "translation-service.terms.v1"
    status: BatchStatus
    provider: str
    model: str
    prompt_version: str
    items_seen: int
    items_completed: int
    items_failed: int
    translations: list[LLMTermTranslation]
    missing_ids: list[str] = Field(default_factory=list)
    error: TranslationError | None = None
    duration_ms: int


class TermTranslationRequestTerm(BaseModel):
    term_key: str = Field(pattern=TERM_KEY_PATTERN)
    source_text: str = Field(min_length=1)
    source_text_normalized: str = Field(min_length=1)


class TermTranslationRequest(BaseModel):
    request_id: str = Field(min_length=1)
    source: str = Field(default="brreg", min_length=1)
    source_lang: str = Field(default="no", min_length=2)
    target_lang: str = Field(default="en", min_length=2)
    provider: str = Field(default="default", min_length=1)
    model: str | None = None
    prompt_version: str = Field(default="v1", min_length=1)
    terms: list[TermTranslationRequestTerm] = Field(min_length=1)

    @model_validator(mode="after")
    def reject_duplicate_term_keys(self) -> "TermTranslationRequest":
        term_keys = [term.term_key for term in self.terms]
        if len(set(term_keys)) != len(term_keys):
            raise ValueError("terms must not contain duplicate term_key values")
        return self


class TermTranslationResultItem(BaseModel):
    term_key: str = Field(pattern=TERM_KEY_PATTERN)
    source_text: str = Field(min_length=1)
    source_text_normalized: str = Field(min_length=1)
    translated_text: str
    status: TermResultStatus = "succeeded"


class TermTranslationFailureResult(BaseModel):
    term_key: str = Field(pattern=TERM_KEY_PATTERN)
    source_text: str = Field(min_length=1)
    source_text_normalized: str = Field(min_length=1)
    status: TermFailureStatus
    error_code: str | None = None
    error: str | None = None


class TermTranslationResponse(BaseModel):
    request_id: str
    source: str
    source_lang: str
    target_lang: str
    provider: str
    model: str | None = None
    prompt_version: str
    results: list[TermTranslationResultItem] = Field(default_factory=list)
    failures: list[TermTranslationFailureResult] = Field(default_factory=list)


class BrregRecordTranslationResult(BaseModel):
    record_id: str
    organization_number: str
    status: RecordStatus
    translated_payload: dict[str, Any] | None = None
    missing_terms: list[str] = Field(default_factory=list)
    error: TranslationError | None = None
    duration_ms: int


class BrregTranslateResponse(BaseModel):
    schema_version: str = "translation-service.brreg.v1"
    status: BatchStatus
    provider: str
    model: str
    prompt_version: str
    records_seen: int
    records_completed: int
    records_failed: int
    records_skipped: int
    duration_ms: int
    results: list[BrregRecordTranslationResult]
