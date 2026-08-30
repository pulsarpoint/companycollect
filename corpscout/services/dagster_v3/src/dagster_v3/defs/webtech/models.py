from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

WEBTECH_EXTENSION_VERSION = "1.3.0"
WEBTECH_DETECTOR_VERSION = f"mywappalyzer-{WEBTECH_EXTENSION_VERSION}"


class ExtensionTechnologyCategory(BaseModel):
    """One category attached to a detected technology."""

    model_config = ConfigDict(extra="forbid")

    id: int = Field(ge=0)
    name: str = Field(min_length=1)
    slug: str = Field(min_length=1)


class ExtensionTechnology(BaseModel):
    """Resolved technology emitted by the packaged extension."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    slug: str = Field(min_length=1)
    categories: list[ExtensionTechnologyCategory]
    confidence: int = Field(ge=0, le=100)
    version: str


class ExtensionReport(BaseModel):
    """The single final report accepted from one browser page."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    analysis_complete: Literal[True]
    extension_version: Literal["1.3.0"]
    page_token: UUID
    url: str = Field(min_length=1)
    technologies: list[ExtensionTechnology]

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
            raise ValueError("url must be an absolute HTTP(S) URL")
        return value


@dataclass(frozen=True, slots=True)
class WebtechCandidate:
    """One ranked Common Crawl root domain selected for scanning."""

    root_domain: str
    harmonic_rank: int


type WebtechOutcome = Literal[
    "success",
    "navigation_error",
    "report_timeout",
    "browser_error",
]


@dataclass(frozen=True, slots=True)
class WebtechDomainResult:
    """Terminal outcome for one candidate domain."""

    candidate: WebtechCandidate
    outcome: WebtechOutcome
    requested_url: str
    final_url: str
    report: ExtensionReport | None
    scanned_at: datetime
    duration_ms: int
    http_fallback_used: bool
    error_message: str

    @classmethod
    def success(
        cls,
        *,
        candidate: WebtechCandidate,
        requested_url: str,
        final_url: str,
        report: ExtensionReport,
        scanned_at: datetime,
        duration_ms: int,
        http_fallback_used: bool = False,
    ) -> Self:
        return cls(
            candidate=candidate,
            outcome="success",
            requested_url=requested_url,
            final_url=final_url,
            report=report,
            scanned_at=scanned_at,
            duration_ms=duration_ms,
            http_fallback_used=http_fallback_used,
            error_message="",
        )

    @classmethod
    def failure(
        cls,
        *,
        candidate: WebtechCandidate,
        outcome: Literal[
            "navigation_error",
            "report_timeout",
            "browser_error",
        ],
        requested_url: str,
        final_url: str,
        scanned_at: datetime,
        duration_ms: int,
        http_fallback_used: bool,
        error_message: str,
    ) -> Self:
        return cls(
            candidate=candidate,
            outcome=outcome,
            requested_url=requested_url,
            final_url=final_url,
            report=None,
            scanned_at=scanned_at,
            duration_ms=duration_ms,
            http_fallback_used=http_fallback_used,
            error_message=error_message,
        )
