import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.webtech.pages import page_identity

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.models import (
    StoredDomainResultDocument,
    StoredResultReference,
)

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.webtech.technologies import (
    WEBTECH_TECHNOLOGY_TABLE,
    load_technology_catalog,
    technology_rows,
)

WEBTECH_CLICKHOUSE_DATABASE = "corpscout"
WEBTECH_RESULT_TABLE = "webtech_domain_scan_results"

WEBTECH_RESULT_COLUMNS = (
    "crawl_id",
    "root_domain",
    "harmonic_rank",
    "detector_version",
    "partition_key",
    "scan_id",
    "run_id",
    "outcome",
    "timeout_stage",
    "extension_failure_stage",
    "requested_url",
    "final_url",
    "final_hostname",
    "http_fallback_used",
    "technology_count",
    "result_bucket",
    "result_object_key",
    "report_sha256",
    "report_size_bytes",
    "scanned_at",
    "duration_ms",
    "error_message",
    "recorded_at",
    "task_id",
    "input_id",
    "website_origin",
    "page_url",
)


@dataclass(frozen=True, slots=True)
class WebtechS3Destination:
    """Bucket and prefix parsed from ``WEBTECH_S3_PATH``."""

    bucket: str
    prefix: str


def parse_webtech_s3_path(value: str) -> WebtechS3Destination:
    """Parse the required ``s3://bucket/prefix`` Webtech root."""
    parsed = urlsplit(value.strip())
    if parsed.scheme != "s3" or parsed.netloc == "":
        raise ValueError("WEBTECH_S3_PATH must use s3://bucket/prefix")
    if parsed.query or parsed.fragment:
        raise ValueError("WEBTECH_S3_PATH must not contain a query or fragment")
    prefix = parsed.path.strip("/")
    if prefix == "" or any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("WEBTECH_S3_PATH must contain a valid prefix")
    return WebtechS3Destination(bucket=parsed.netloc, prefix=prefix)


def index_result_references(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    crawl_id: str,
    detector_version: str,
    references: Sequence[StoredResultReference],
    dagster_run_id: str,
) -> int:
    """Publish stored page results of one execution, from any of its envelopes."""
    if not references:
        return 0
    unique_refs = _unique_references(references)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=WEBTECH_CLICKHOUSE_DATABASE,
        tables=[
            WEBTECH_RESULT_TABLE,
            WEBTECH_TECHNOLOGY_TABLE,
            "technology_catalog",
            "technology_aliases",
        ],
    )
    with clickhouse.get_connection() as client:
        catalog = load_technology_catalog(client)
    recorded_at = datetime.now(UTC)
    rows, detections = [], []
    for reference in unique_refs:
        body = object_store.read_bytes(reference.object_key, bucket=destination.bucket)
        _validate_result_body(reference, body)
        stored = StoredDomainResultDocument.model_validate_json(body)
        _validate_execution_result_identity(
            stored,
            reference=reference,
            crawl_id=crawl_id,
            detector_version=detector_version,
        )
        detections.extend(
            technology_rows(
                stored,
                reference,
                catalog,
                bucket=destination.bucket,
                run_id=dagster_run_id,
                recorded_at=recorded_at,
            )
        )
        rows.append(
            _clickhouse_row(
                stored,
                result_reference=reference,
                dagster_run_id=dagster_run_id,
                recorded_at=recorded_at,
                result_bucket=destination.bucket,
            )
        )
    with clickhouse.get_connection() as client:
        from dagster_v3.defs.webtech.writes import publish_scan_rows

        publish_scan_rows(client, rows, detections)
    return len(rows)


def _unique_references(
    references: Sequence[StoredResultReference],
) -> list[StoredResultReference]:
    """Deduplicate references by input_id; identical duplicates collapse, differing ones raise."""
    seen: dict[str, StoredResultReference] = {}
    for reference in references:
        if reference.input_id in seen:
            if seen[reference.input_id] != reference:
                raise ValueError(
                    f"conflicting result references for input {reference.input_id}"
                )
        else:
            seen[reference.input_id] = reference
    return list(seen.values())


def _matches_reference(
    document: StoredDomainResultDocument,
    reference: StoredResultReference,
) -> bool:
    """Check if document matches the reference on shared per-page fields."""
    return (
        document.candidate.input_id == reference.input_id
        and document.candidate.root_domain == reference.root_domain
        and document.outcome == reference.outcome
        and _technology_count(document.report) == reference.technology_count
    )


def _validate_execution_result_identity(
    document: StoredDomainResultDocument,
    *,
    reference: StoredResultReference,
    crawl_id: str,
    detector_version: str,
) -> None:
    # The scan and envelope may differ: pages are reused across envelopes of an execution.
    if (
        document.crawl_id != crawl_id
        or document.detector_version != detector_version
        or not reference.input_id
        or not _matches_reference(document, reference)
    ):
        raise ValueError(f"result identity mismatch: {reference.object_key}")


def _validate_result_body(
    reference: StoredResultReference,
    body: bytes,
) -> None:
    if len(body) != reference.size_bytes:
        raise ValueError(f"result size mismatch: {reference.object_key}")
    if hashlib.sha256(body).hexdigest() != reference.sha256:
        raise ValueError(f"result SHA-256 mismatch: {reference.object_key}")


def _clickhouse_row(
    document: StoredDomainResultDocument,
    *,
    result_reference: StoredResultReference,
    dagster_run_id: str,
    recorded_at: datetime,
    result_bucket: str,
) -> tuple[object, ...]:
    return (
        document.crawl_id,
        document.candidate.root_domain,
        document.candidate.harmonic_rank,
        document.detector_version,
        document.partition_key,
        document.scan_id,
        dagster_run_id,
        document.outcome,
        document.timeout_stage or "",
        _extension_failure_stage(document.report),
        document.requested_url,
        document.final_url,
        document.final_hostname,
        int(document.http_fallback_used),
        result_reference.technology_count,
        result_bucket,
        result_reference.object_key,
        result_reference.sha256,
        result_reference.size_bytes,
        document.scanned_at,
        document.duration_ms,
        document.error_message[:2_000],
        recorded_at,
        document.candidate.task_id,
        document.candidate.input_id,
        *page_identity(document.candidate.page_url or document.requested_url),
    )


def _technology_count(report: dict[str, object] | None) -> int:
    if report is None:
        return 0
    technologies = report.get("technologies")
    if not isinstance(technologies, list):
        raise ValueError("extension report technologies must be a list")
    return len(technologies)


def _extension_failure_stage(report: dict[str, object] | None) -> str:
    if report is None:
        return ""
    failure_stage = report.get("failure_stage")
    if failure_stage is None:
        return ""
    if not isinstance(failure_stage, str):
        raise ValueError("extension report failure_stage must be a string or null")
    return failure_stage
