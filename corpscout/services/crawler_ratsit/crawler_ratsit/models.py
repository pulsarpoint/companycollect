from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Self
from uuid import UUID

from crawler_ratsit.constants import RESPONSE_SCHEMA_VERSION, SOURCE_NAME


CRAWL_OUTCOMES = frozenset(
    {"success", "not_found", "retry_exhausted", "blocked", "selector_changed"}
)
PAGE_OUTCOMES = frozenset({"success", "not_found", "blocked", "selector_changed"})


@dataclass(frozen=True)
class CrawlCompanyInput:
    company_id: str
    batch_id: str

    def __post_init__(self) -> None:
        validate_company_id(self.company_id)
        validate_batch_id(self.batch_id)


@dataclass(frozen=True)
class CrawlActivityInput:
    company_id: str
    batch_id: str
    selected_at: str
    temporal_workflow_id: str
    temporal_run_id: str

    def __post_init__(self) -> None:
        validate_company_id(self.company_id)
        validate_batch_id(self.batch_id)
        validate_utc_timestamp(self.selected_at, field_name="selected_at")
        if not self.temporal_workflow_id:
            raise ValueError("temporal_workflow_id must not be blank")
        if not self.temporal_run_id:
            raise ValueError("temporal_run_id must not be blank")


@dataclass(frozen=True)
class FetchedPage:
    outcome: str
    requested_url: str
    final_url: str
    http_status: int | None
    content: str
    error_type: str
    error_message: str

    def __post_init__(self) -> None:
        if self.outcome not in PAGE_OUTCOMES:
            raise ValueError(f"unsupported page outcome: {self.outcome}")
        if self.outcome == "success" and not self.content:
            raise ValueError("a successful page must contain content")


@dataclass(frozen=True)
class CrawlResult:
    company_id: str
    batch_id: str
    outcome: str
    selected_at: str
    attempted_at: str
    completed_at: str
    http_status: int | None
    source_url: str
    source_bucket: str
    source_object_key: str
    content_size_bytes: int
    duration_ms: int
    attempt_count: int
    error_type: str
    error_message: str
    temporal_workflow_id: str
    temporal_run_id: str

    def __post_init__(self) -> None:
        validate_company_id(self.company_id)
        validate_batch_id(self.batch_id)
        if self.outcome not in CRAWL_OUTCOMES:
            raise ValueError(f"unsupported crawl outcome: {self.outcome}")

        selected_at = validate_utc_timestamp(
            self.selected_at,
            field_name="selected_at",
        )
        attempted_at = validate_utc_timestamp(
            self.attempted_at,
            field_name="attempted_at",
        )
        completed_at = validate_utc_timestamp(
            self.completed_at,
            field_name="completed_at",
        )
        if not selected_at <= attempted_at <= completed_at:
            raise ValueError(
                "crawl timestamps must satisfy selected_at <= attempted_at <= completed_at"
            )

        if self.source_url != ratsit_url(self.company_id):
            raise ValueError("source_url does not match company_id")
        if self.source_object_key and not self.source_bucket:
            raise ValueError(
                "source_bucket is required when source_object_key is present"
            )
        if self.content_size_bytes < 0:
            raise ValueError("content_size_bytes must not be negative")
        if self.content_size_bytes > 0 and not self.source_object_key:
            raise ValueError("content requires an S3 object location")
        if self.duration_ms < 0:
            raise ValueError("duration_ms must not be negative")
        if self.attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        if not self.temporal_workflow_id:
            raise ValueError("temporal_workflow_id must not be blank")
        if not self.temporal_run_id:
            raise ValueError("temporal_run_id must not be blank")

        if self.outcome == "success":
            if self.http_status is None or not 200 <= self.http_status < 300:
                raise ValueError("a successful crawl requires a 2xx HTTP status")
            if not self.source_object_key:
                raise ValueError("a successful crawl requires an S3 object")
            if self.content_size_bytes == 0:
                raise ValueError("a successful crawl requires non-empty content")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        return cls(**value)


def ratsit_url(company_id: str) -> str:
    validate_company_id(company_id)
    return f"https://www.ratsit.se/{company_id}"


def validate_company_id(company_id: str) -> None:
    if (
        not isinstance(company_id, str)
        or len(company_id) != 10
        or not company_id.isascii()
        or not company_id.isdigit()
    ):
        raise ValueError("company_id must contain exactly ten ASCII digits")


def validate_batch_id(batch_id: str) -> None:
    if not isinstance(batch_id, str):
        raise ValueError("batch_id must be a UUID")
    try:
        UUID(batch_id)
    except ValueError as error:
        raise ValueError("batch_id must be a UUID") from error


def validate_utc_timestamp(value: str, *, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return parsed


def response_envelope(
    result: CrawlResult,
    *,
    final_url: str,
    content: str,
) -> dict[str, Any]:
    content_size_bytes = len(content.encode("utf-8"))
    if content_size_bytes != result.content_size_bytes:
        raise ValueError("response content size does not match crawl result")
    return {
        "schema_version": RESPONSE_SCHEMA_VERSION,
        "source": SOURCE_NAME,
        "result": result.to_dict(),
        "final_url": final_url,
        "content_type": "text/html",
        "content": content,
    }


def result_from_response_envelope(
    value: dict[str, Any],
    *,
    expected_company_id: str,
    expected_batch_id: str,
) -> CrawlResult:
    if value.get("schema_version") != RESPONSE_SCHEMA_VERSION:
        raise ValueError("unsupported Ratsit response schema version")
    if value.get("source") != SOURCE_NAME:
        raise ValueError("S3 object is not a Ratsit response")

    raw_result = value.get("result")
    if not isinstance(raw_result, dict):
        raise ValueError("Ratsit response result must be an object")
    result = CrawlResult.from_dict(raw_result)
    if result.company_id != expected_company_id:
        raise ValueError("S3 response company_id does not match its object key")
    if result.batch_id != expected_batch_id:
        raise ValueError("S3 response batch_id does not match its object key")

    content = value.get("content")
    if not isinstance(content, str):
        raise ValueError("Ratsit response content must be a string")
    if len(content.encode("utf-8")) != result.content_size_bytes:
        raise ValueError("S3 response content size does not match its result metadata")
    return result
