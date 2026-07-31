from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AnnualAccountWord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str
    bbox: tuple[float, float, float, float]
    confidence: float
    block_number: int
    paragraph_number: int
    line_number: int
    word_number: int

    @field_validator("bbox")
    @classmethod
    def validate_bbox(
        cls,
        value: tuple[float, float, float, float],
    ) -> tuple[float, float, float, float]:
        left, top, right, bottom = value
        if not (0 <= left <= right <= 1 and 0 <= top <= bottom <= 1):
            raise ValueError(f"bbox must use normalized page coordinates: {value}")
        return value


class AnnualAccountPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page_number: int = Field(gt=0)
    extraction_method: Literal["native_text", "tesseract_ocr"]
    width: float = Field(gt=0)
    height: float = Field(gt=0)
    text: str
    mean_word_confidence: float | None
    words: list[AnnualAccountWord]


class AnnualAccountExtraction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pdf_engine: str
    pdf_engine_version: str
    ocr_engine: str
    ocr_languages: str
    ocr_page_segmentation_mode: int
    bbox_coordinate_space: Literal["normalized_page"]


class AnnualAccountDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int
    document_id: str
    country_iso2: Literal["NO"]
    source_system: str
    source_run_id: str
    org_number: str
    legal_name: str
    filing_year: int
    source_file_name: str | None = None
    source_pdf_url: str
    source_pdf_sha256: str
    source_pdf_size_bytes: int = Field(ge=0)
    retrieved_at: str
    pdf_page_count: int = Field(ge=0)
    native_text_page_count: int = Field(ge=0)
    ocr_page_count: int = Field(ge=0)
    extraction: AnnualAccountExtraction
    pages: list[AnnualAccountPage]

    @field_validator("source_pdf_sha256")
    @classmethod
    def validate_source_pdf_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("source_pdf_sha256 must be lowercase SHA-256 hex")
        return value


class ExtractedAnnualAccountFact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fact_id: str
    document_id: str
    org_number: str
    source_filing_year: int
    source_chunk: str = ""
    fact_ordinal: int
    page_number: int
    line_number: int
    statement_type: str
    table_title: str
    raw_label: str
    normalized_label: str
    canonical_concept: str | None = None
    column_label: str
    fiscal_year: int | None
    period_end_date: str | None
    is_comparative: bool
    value_kind: str
    raw_value: str
    numeric_value: Decimal
    currency: str
    unit_scale: Decimal
    amount_original: Decimal | None
    amount_usd: Decimal | None = None
    fx_rate_to_usd: Decimal | None = None
    fx_rate_date: str | None = None
    fx_source: str | None = None
    bbox: tuple[float, float, float, float]
    evidence: str
    ocr_confidence: float
    extraction_method: str
    mapping_method: str = "unmapped"
    mapping_confidence: float | None = None
    quality_flags: tuple[str, ...] = ()
    source_json_sha256: str
    parser_version: str


class AnnualAccountConceptMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_id: int
    canonical_concept: str | None
    confidence: float = Field(ge=0, le=1)


class AnnualAccountConceptMappingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mappings: list[AnnualAccountConceptMapping]
