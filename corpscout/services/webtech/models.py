from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

WEBTECH_EXTENSION_VERSION = "1.4.0"
WEBTECH_DETECTOR_VERSION = f"mywappalyzer-{WEBTECH_EXTENSION_VERSION}"

type ExtensionAnalysisStatus = Literal["complete", "partial", "failed"]
type ExtensionAnalysisStage = Literal[
    "content_started",
    "initial_metadata",
    "initial_dom_js",
    "heavy_signals",
    "delayed_wait",
    "delayed_dom_js",
    "finalizing",
]
type ExtensionTimingStage = Literal[
    "content_started",
    "initial_metadata",
    "initial_dom_js",
    "heavy_signals",
    "delayed_wait",
    "delayed_dom_js",
    "finalizing",
    "analysis_failed",
    "analysis_timed_out",
    "report_post_started",
]


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
    """The single terminal report accepted from one browser page."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[3]
    analysis_complete: bool
    analysis_status: ExtensionAnalysisStatus
    extension_version: Literal["1.4.0"]
    page_token: UUID
    url: str = Field(min_length=1)
    technologies: list[ExtensionTechnology]
    failure_stage: ExtensionAnalysisStage | None
    error_message: str
    stage_timings_ms: dict[
        ExtensionTimingStage,
        Annotated[int, Field(ge=0)],
    ]

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
            raise ValueError("url must be an absolute HTTP(S) URL")
        return value

    @model_validator(mode="after")
    def validate_terminal_status(self) -> Self:
        if self.analysis_status == "complete":
            if not self.analysis_complete:
                raise ValueError("complete reports require analysis_complete=true")
            if self.failure_stage is not None or self.error_message != "":
                raise ValueError("complete reports must not contain failure details")
            return self

        if self.analysis_complete:
            raise ValueError("partial and failed reports require analysis_complete=false")
        if self.failure_stage is None or self.error_message == "":
            raise ValueError("partial and failed reports require failure details")
        return self


@dataclass(frozen=True, slots=True)
class WebtechCandidate:
    """One ranked Common Crawl root domain selected for scanning."""

    root_domain: str
    harmonic_rank: int


type WebtechOutcome = Literal[
    "success",
    "extension_error",
    "navigation_error",
    "hard_timeout",
    "browser_error",
]

type WebtechTimeoutStage = Literal[
    "page_creation",
    "https_navigation",
    "http_navigation",
    "extension_token",
    "wappalyzer_report",
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
    timeout_stage: WebtechTimeoutStage | None = None

    def __post_init__(self) -> None:
        if self.outcome == "hard_timeout" and self.timeout_stage is None:
            raise ValueError("hard_timeout results require a timeout_stage")
        if self.outcome != "hard_timeout" and self.timeout_stage is not None:
            raise ValueError("only hard_timeout results may have a timeout_stage")
        if self.outcome in {"success", "extension_error"} and self.report is None:
            raise ValueError("extension outcomes require an extension report")
        if self.outcome not in {"success", "extension_error"} and self.report is not None:
            raise ValueError("browser and navigation failures must not have a report")
        if self.outcome == "success" and not self.report.analysis_complete:
            raise ValueError("success results require a complete extension report")
        if self.outcome == "extension_error" and self.report.analysis_complete:
            raise ValueError("extension_error results require a non-complete report")

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
    def extension_error(
        cls,
        *,
        candidate: WebtechCandidate,
        requested_url: str,
        final_url: str,
        report: ExtensionReport,
        scanned_at: datetime,
        duration_ms: int,
        http_fallback_used: bool,
    ) -> Self:
        return cls(
            candidate=candidate,
            outcome="extension_error",
            requested_url=requested_url,
            final_url=final_url,
            report=report,
            scanned_at=scanned_at,
            duration_ms=duration_ms,
            http_fallback_used=http_fallback_used,
            error_message=report.error_message,
        )

    @classmethod
    def failure(
        cls,
        *,
        candidate: WebtechCandidate,
        outcome: Literal[
            "navigation_error",
            "hard_timeout",
            "browser_error",
        ],
        requested_url: str,
        final_url: str,
        scanned_at: datetime,
        duration_ms: int,
        http_fallback_used: bool,
        error_message: str,
        timeout_stage: WebtechTimeoutStage | None = None,
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
            timeout_stage=timeout_stage,
        )
