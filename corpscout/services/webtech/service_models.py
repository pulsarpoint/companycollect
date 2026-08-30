import re
from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from models import (
    WEBTECH_DETECTOR_VERSION,
    ExtensionReport,
    WebtechOutcome,
    WebtechTimeoutStage,
)

CRAWL_ID_PATTERN = re.compile(r"CC-MAIN-[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
SHA256_PATTERN = re.compile(r"[a-f0-9]{64}")

type ScanStatus = Literal[
    "pending",
    "running",
    "completed",
    "failed",
    "cancelled",
]


class Candidate(BaseModel):
    """One ranked root domain in a scanner input manifest."""

    model_config = ConfigDict(extra="forbid")

    root_domain: str = Field(min_length=1, max_length=253)
    harmonic_rank: int = Field(ge=1)

    @field_validator("root_domain")
    @classmethod
    def validate_root_domain(cls, value: str) -> str:
        if (
            value != value.strip().lower()
            or "/" in value
            or ".." in value
            or value.startswith(".")
            or value.endswith(".")
        ):
            raise ValueError("root_domain must be a normalized hostname")
        return value


class CandidateManifest(BaseModel):
    """Immutable Dagster-to-scanner handoff stored in RustFS."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    crawl_id: str
    partition_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    detector_version: Literal[WEBTECH_DETECTOR_VERSION]
    dagster_run_id: str = Field(min_length=1, max_length=128)
    generated_at: datetime
    candidates: list[Candidate] = Field(max_length=1_000_000)

    @field_validator("crawl_id")
    @classmethod
    def validate_crawl_id(cls, value: str) -> str:
        if CRAWL_ID_PATTERN.fullmatch(value) is None:
            raise ValueError("crawl_id must be a valid Common Crawl ID")
        return value

    @model_validator(mode="after")
    def validate_candidates(self) -> Self:
        domains = [candidate.root_domain for candidate in self.candidates]
        if len(domains) != len(set(domains)):
            raise ValueError("candidate manifest contains duplicate root domains")
        return self


class ScanRequest(BaseModel):
    """Idempotent request submitted by Dagster."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    crawl_id: str
    partition_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    candidate_manifest_uri: str = Field(pattern=r"^s3://[^/]+/.+")
    candidate_manifest_sha256: str
    detector_version: Literal[WEBTECH_DETECTOR_VERSION]

    @field_validator("crawl_id")
    @classmethod
    def validate_crawl_id(cls, value: str) -> str:
        if CRAWL_ID_PATTERN.fullmatch(value) is None:
            raise ValueError("crawl_id must be a valid Common Crawl ID")
        return value

    @field_validator("candidate_manifest_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if SHA256_PATTERN.fullmatch(value) is None:
            raise ValueError("candidate_manifest_sha256 must be lowercase SHA-256")
        return value


class StoredResultReference(BaseModel):
    """Final RustFS object identity and summary for one domain."""

    model_config = ConfigDict(extra="forbid")

    root_domain: str
    harmonic_rank: int
    outcome: str
    timeout_stage: str | None
    technology_count: int = Field(ge=0)
    duration_ms: int = Field(ge=0)
    object_key: str
    sha256: str
    size_bytes: int = Field(ge=0)


class StoredDomainResultDocument(BaseModel):
    """Canonical per-domain scanner result stored in RustFS."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    scan_id: str
    crawl_id: str
    partition_key: str
    detector_version: str
    candidate: Candidate
    outcome: WebtechOutcome
    requested_url: str
    final_url: str
    final_hostname: str
    http_fallback_used: bool
    scanned_at: datetime
    duration_ms: int = Field(ge=0)
    error_message: str
    timeout_stage: WebtechTimeoutStage | None
    report: ExtensionReport | None


class ScanProgressEvent(BaseModel):
    """Compact terminal-outcome window emitted to Dagster."""

    model_config = ConfigDict(extra="forbid")

    sequence: int = Field(ge=1)
    completed_count: int = Field(ge=0)
    total_count: int = Field(ge=0)
    window_count: int = Field(ge=1)
    window_outcome_counts: dict[str, int]
    window_technology_count: int = Field(ge=0)
    elapsed_seconds: float = Field(ge=0)
    domains_per_minute: float = Field(ge=0)


class ScanSnapshot(BaseModel):
    """Current state returned by both submit and long-poll requests."""

    model_config = ConfigDict(extra="forbid")

    scan_id: str
    status: ScanStatus
    crawl_id: str
    partition_key: str
    detector_version: str
    candidate_manifest_uri: str
    result_prefix_uri: str
    final_manifest_uri: str
    total_count: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    outcome_counts: dict[str, int]
    technology_count: int = Field(ge=0)
    started_at: datetime | None
    finished_at: datetime | None
    elapsed_seconds: float = Field(ge=0)
    domains_per_minute: float = Field(ge=0)
    latest_event_sequence: int = Field(ge=0)
    error_message: str


class ScanPollResponse(BaseModel):
    """Long-poll response containing only events newer than the cursor."""

    model_config = ConfigDict(extra="forbid")

    scan: ScanSnapshot
    events: list[ScanProgressEvent]


class FinalScanManifest(BaseModel):
    """Completion marker written only after all domain objects exist."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    scan_id: str
    crawl_id: str
    partition_key: str
    detector_version: str
    candidate_manifest_uri: str
    candidate_manifest_sha256: str
    started_at: datetime
    finished_at: datetime
    elapsed_seconds: float = Field(ge=0)
    outcome_counts: dict[str, int]
    technology_count: int = Field(ge=0)
    scanner_settings: dict[str, object]
    results: list[StoredResultReference]
