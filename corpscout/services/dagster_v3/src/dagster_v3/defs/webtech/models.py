from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

WEBTECH_EXTENSION_VERSION = "1.4.1"
WEBTECH_DETECTOR_VERSION = f"mywappalyzer-{WEBTECH_EXTENSION_VERSION}"

type RemoteScanStatus = Literal[
    "pending",
    "running",
    "completed",
    "failed",
    "cancelled",
]


class WebtechCandidate(BaseModel):
    """One ranked Common Crawl root domain selected for scanning."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    root_domain: str = Field(min_length=1, max_length=253)
    harmonic_rank: int = Field(ge=1)


class CandidateManifestDocument(BaseModel):
    """Immutable input sent to the remote scanner through RustFS."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    crawl_id: str
    partition_key: str
    detector_version: Literal[WEBTECH_DETECTOR_VERSION]
    dagster_run_id: str
    generated_at: datetime
    candidates: list[WebtechCandidate]


@dataclass(frozen=True, slots=True)
class CandidateManifestReference:
    """Dagster output pointing at one immutable candidate manifest."""

    crawl_id: str
    partition_key: str
    detector_version: str
    dagster_run_id: str
    uri: str
    sha256: str
    candidate_count: int


class StoredResultReference(BaseModel):
    """One result object entry in the remote final manifest."""

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


class FinalScanManifest(BaseModel):
    """Remote completion marker consumed by the ClickHouse index asset."""

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


class StoredDomainResultDocument(BaseModel):
    """Per-domain RustFS result fields indexed into ClickHouse."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal[1]
    scan_id: str
    crawl_id: str
    partition_key: str
    detector_version: str
    candidate: WebtechCandidate
    outcome: str
    requested_url: str
    final_url: str
    final_hostname: str
    http_fallback_used: bool
    scanned_at: datetime
    duration_ms: int = Field(ge=0)
    error_message: str
    timeout_stage: str | None
    report: dict[str, object] | None


@dataclass(frozen=True, slots=True)
class FinalScanReference:
    """Dagster output pointing at a completed remote scan manifest."""

    scan_id: str
    crawl_id: str
    partition_key: str
    detector_version: str
    uri: str
    total_count: int
    outcome_counts: dict[str, int]
    technology_count: int
    elapsed_seconds: float
    domains_per_minute: float


class RemoteScanProgressEvent(BaseModel):
    """One compact progress window returned by the scanner API."""

    model_config = ConfigDict(extra="forbid")

    sequence: int
    completed_count: int
    total_count: int
    window_count: int
    window_outcome_counts: dict[str, int]
    window_technology_count: int
    elapsed_seconds: float
    domains_per_minute: float


class RemoteScanSnapshot(BaseModel):
    """Current remote scan state."""

    model_config = ConfigDict(extra="forbid")

    scan_id: str
    status: RemoteScanStatus
    crawl_id: str
    partition_key: str
    detector_version: str
    candidate_manifest_uri: str
    result_prefix_uri: str
    final_manifest_uri: str
    total_count: int
    completed_count: int
    outcome_counts: dict[str, int]
    technology_count: int
    started_at: datetime | None
    finished_at: datetime | None
    elapsed_seconds: float
    domains_per_minute: float
    latest_event_sequence: int
    error_message: str


class RemoteScanPollResponse(BaseModel):
    """Cursor-based scanner long-poll response."""

    model_config = ConfigDict(extra="forbid")

    scan: RemoteScanSnapshot
    events: list[RemoteScanProgressEvent]
