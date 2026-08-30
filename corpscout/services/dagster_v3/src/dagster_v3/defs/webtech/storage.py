import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    CandidateManifestDocument,
    CandidateManifestReference,
    FinalScanManifest,
    FinalScanReference,
    StoredDomainResultDocument,
    StoredResultReference,
    WebtechCandidate,
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
    if prefix == "" or any(
        part in {"", ".", ".."} for part in prefix.split("/")
    ):
        raise ValueError("WEBTECH_S3_PATH must contain a valid prefix")
    return WebtechS3Destination(bucket=parsed.netloc, prefix=prefix)


def write_candidate_manifest(
    *,
    object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    crawl_id: str,
    partition_key: str,
    dagster_run_id: str,
    candidates: tuple[WebtechCandidate, ...],
) -> CandidateManifestReference:
    """Write or reuse the immutable handoff for one partition selection."""
    object_store.ensure_bucket(destination.bucket)
    object_key = "/".join(
        (
            destination.prefix,
            "candidates",
            f"detector_version={WEBTECH_DETECTOR_VERSION}",
            f"crawl_id={crawl_id}",
            f"partition_key={partition_key}",
            f"dagster_run_id={dagster_run_id}",
            "manifest.json",
        )
    )
    existing_body: bytes | None = None
    if object_store.exists(object_key, bucket=destination.bucket):
        existing_body = object_store.read_bytes(
            object_key,
            bucket=destination.bucket,
        )
        existing = CandidateManifestDocument.model_validate_json(existing_body)
        if (
            existing.crawl_id != crawl_id
            or existing.partition_key != partition_key
            or existing.dagster_run_id != dagster_run_id
            or tuple(existing.candidates) != candidates
        ):
            existing_body = None

    if existing_body is None:
        document = CandidateManifestDocument(
            schema_version=2,
            crawl_id=crawl_id,
            partition_key=partition_key,
            detector_version=WEBTECH_DETECTOR_VERSION,
            dagster_run_id=dagster_run_id,
            generated_at=datetime.now(UTC),
            candidates=list(candidates),
        )
        body = _json_bytes(document.model_dump(mode="json"))
        object_store.write_json(
            object_key,
            body.decode("utf-8"),
            bucket=destination.bucket,
        )
    else:
        body = existing_body

    return CandidateManifestReference(
        crawl_id=crawl_id,
        partition_key=partition_key,
        detector_version=WEBTECH_DETECTOR_VERSION,
        dagster_run_id=dagster_run_id,
        uri=f"s3://{destination.bucket}/{object_key}",
        sha256=hashlib.sha256(body).hexdigest(),
        candidate_count=len(candidates),
    )


def read_final_manifest(
    *,
    object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    reference: FinalScanReference,
) -> FinalScanManifest:
    """Read and validate the completion marker returned by the remote API."""
    bucket, key = _parse_allowed_uri(reference.uri, destination=destination)
    manifest = FinalScanManifest.model_validate_json(
        object_store.read_bytes(key, bucket=bucket)
    )
    if (
        manifest.scan_id != reference.scan_id
        or manifest.crawl_id != reference.crawl_id
        or manifest.partition_key != reference.partition_key
        or manifest.detector_version != reference.detector_version
    ):
        raise ValueError("final manifest identity does not match the Dagster output")
    if len(manifest.results) != reference.total_count:
        raise ValueError("final manifest result count does not match the remote snapshot")
    return manifest


def index_final_results(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    reference: FinalScanReference,
    dagster_run_id: str,
) -> int:
    """Validate all durable results, then insert their ClickHouse index rows."""
    manifest = read_final_manifest(
        object_store=object_store,
        destination=destination,
        reference=reference,
    )
    recorded_at = datetime.now(UTC)
    rows = []
    seen_domains: set[str] = set()
    for result_reference in manifest.results:
        if result_reference.root_domain in seen_domains:
            raise ValueError(
                f"duplicate result in final manifest: {result_reference.root_domain}"
            )
        seen_domains.add(result_reference.root_domain)
        body = object_store.read_bytes(
            result_reference.object_key,
            bucket=destination.bucket,
        )
        _validate_result_body(result_reference, body)
        document = StoredDomainResultDocument.model_validate_json(body)
        _validate_result_identity(
            document,
            result_reference=result_reference,
            manifest=manifest,
        )
        rows.append(
            _clickhouse_row(
                document,
                result_reference=result_reference,
                dagster_run_id=dagster_run_id,
                recorded_at=recorded_at,
                result_bucket=destination.bucket,
            )
        )

    if rows:
        with clickhouse.get_connection() as client:
            client.execute(
                f"""
                INSERT INTO {WEBTECH_CLICKHOUSE_DATABASE}.{WEBTECH_RESULT_TABLE}
                ({", ".join(WEBTECH_RESULT_COLUMNS)}) VALUES
                """,
                rows,
            )
    return len(rows)


def _validate_result_body(
    reference: StoredResultReference,
    body: bytes,
) -> None:
    if len(body) != reference.size_bytes:
        raise ValueError(f"result size mismatch: {reference.object_key}")
    if hashlib.sha256(body).hexdigest() != reference.sha256:
        raise ValueError(f"result SHA-256 mismatch: {reference.object_key}")


def _validate_result_identity(
    document: StoredDomainResultDocument,
    *,
    result_reference: StoredResultReference,
    manifest: FinalScanManifest,
) -> None:
    if (
        document.scan_id != manifest.scan_id
        or document.crawl_id != manifest.crawl_id
        or document.partition_key != manifest.partition_key
        or document.detector_version != manifest.detector_version
        or document.candidate.root_domain != result_reference.root_domain
        or document.candidate.harmonic_rank != result_reference.harmonic_rank
        or document.outcome != result_reference.outcome
        or _technology_count(document.report) != result_reference.technology_count
    ):
        raise ValueError(
            f"result identity mismatch: {result_reference.object_key}"
        )


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


def _parse_allowed_uri(
    uri: str,
    *,
    destination: WebtechS3Destination,
) -> tuple[str, str]:
    parsed = urlsplit(uri)
    key = parsed.path.strip("/")
    if (
        parsed.scheme != "s3"
        or parsed.netloc != destination.bucket
        or not key.startswith(f"{destination.prefix}/")
    ):
        raise ValueError("Webtech object is outside WEBTECH_S3_PATH")
    return parsed.netloc, key


def _json_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
