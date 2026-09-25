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
    """One page of a queue envelope sent to the scanner."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    root_domain: str = Field(min_length=1, max_length=253)
    harmonic_rank: int = Field(default=0, ge=0)
    task_id: str = ""
    input_id: str = ""
    page_url: str = ""


class StoredResultReference(BaseModel):
    """One stored page report named by a scanner progress event."""

    model_config = ConfigDict(extra="forbid")

    root_domain: str
    harmonic_rank: int
    input_id: str = ""
    outcome: str
    timeout_stage: str | None
    technology_count: int = Field(ge=0)
    duration_ms: int = Field(ge=0)
    object_key: str
    sha256: str
    size_bytes: int = Field(ge=0)


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
    # Stored page results in this window.
    results: list[StoredResultReference] = Field(default_factory=list)


class RemoteScanSnapshot(BaseModel):
    """Current remote scan state."""

    model_config = ConfigDict(extra="forbid")

    scan_id: str
    status: RemoteScanStatus
    crawl_id: str
    detector_version: str
    total_count: int
    completed_count: int
    outcome_counts: dict[str, int]
    technology_count: int
    started_at: datetime | None
    finished_at: datetime | None
    last_progress_at: datetime | None
    elapsed_seconds: float
    progress_age_seconds: float
    domains_per_minute: float
    latest_event_sequence: int
    error_message: str


class RemoteScanPollResponse(BaseModel):
    """Cursor-based scanner long-poll response."""

    model_config = ConfigDict(extra="forbid")

    scan: RemoteScanSnapshot
    events: list[RemoteScanProgressEvent]
