import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    WebtechDomainResult,
)

WEBTECH_CLICKHOUSE_DATABASE = "corpscout"
WEBTECH_RESULT_TABLE = "webtech_domain_scan_results"
WEBTECH_STORED_SCHEMA_VERSION = 1

WEBTECH_RESULT_COLUMNS = (
    "crawl_id",
    "root_domain",
    "harmonic_rank",
    "detector_version",
    "partition_key",
    "run_id",
    "outcome",
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


@dataclass(frozen=True, slots=True)
class StoredWebtechResult:
    """S3 identity and checksum for one persisted domain outcome."""

    result: WebtechDomainResult
    bucket: str
    object_key: str
    sha256: str
    size_bytes: int

    @property
    def s3_uri(self) -> str:
        return f"s3://{self.bucket}/{self.object_key}"


def parse_webtech_s3_path(value: str) -> WebtechS3Destination:
    """Parse the pilot's required ``s3://bucket/prefix`` destination."""
    parsed = urlsplit(value.strip())
    if parsed.scheme != "s3" or parsed.netloc == "":
        raise ValueError("WEBTECH_S3_PATH must use s3://bucket/prefix")
    if parsed.query or parsed.fragment:
        raise ValueError("WEBTECH_S3_PATH must not contain a query or fragment")

    prefix = parsed.path.strip("/")
    if prefix and any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("WEBTECH_S3_PATH contains an invalid prefix")
    return WebtechS3Destination(bucket=parsed.netloc, prefix=prefix)


def webtech_result_object_key(
    *,
    destination: WebtechS3Destination,
    crawl_id: str,
    root_domain: str,
) -> str:
    """Return the deterministic RustFS key for one crawl/domain pair."""
    _validate_crawl_id(crawl_id)
    _validate_root_domain(root_domain)
    parts = (
        destination.prefix,
        f"crawl_id={crawl_id}",
        f"root_domain={root_domain.lower()}",
        "report.json",
    )
    return "/".join(part for part in parts if part)


def persist_webtech_results(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    crawl_id: str,
    partition_key: str,
    run_id: str,
    results: tuple[WebtechDomainResult, ...],
) -> tuple[StoredWebtechResult, ...]:
    """Write every outcome to RustFS, then index the completed writes in ClickHouse."""
    if not results:
        return ()

    _validate_crawl_id(crawl_id)
    object_store.ensure_bucket(destination.bucket)
    stored_results: list[StoredWebtechResult] = []
    for result in results:
        object_key = webtech_result_object_key(
            destination=destination,
            crawl_id=crawl_id,
            root_domain=result.candidate.root_domain,
        )
        document = _stored_document(
            result,
            crawl_id=crawl_id,
            partition_key=partition_key,
            run_id=run_id,
        )
        body = json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        object_store.write_json(
            object_key,
            body,
            bucket=destination.bucket,
        )
        stored_results.append(
            StoredWebtechResult(
                result=result,
                bucket=destination.bucket,
                object_key=object_key,
                sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
                size_bytes=len(body.encode("utf-8")),
            )
        )

    recorded_at = datetime.now(UTC)
    rows = [
        _clickhouse_row(
            stored,
            crawl_id=crawl_id,
            partition_key=partition_key,
            run_id=run_id,
            recorded_at=recorded_at,
        )
        for stored in stored_results
    ]
    with clickhouse.get_connection() as client:
        client.execute(
            f"""
            INSERT INTO {WEBTECH_CLICKHOUSE_DATABASE}.{WEBTECH_RESULT_TABLE}
            ({", ".join(WEBTECH_RESULT_COLUMNS)}) VALUES
            """,
            rows,
        )
    return tuple(stored_results)


def _stored_document(
    result: WebtechDomainResult,
    *,
    crawl_id: str,
    partition_key: str,
    run_id: str,
) -> dict[str, object]:
    return {
        "schema_version": WEBTECH_STORED_SCHEMA_VERSION,
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "crawl_id": crawl_id,
        "partition_key": partition_key,
        "run_id": run_id,
        "candidate": {
            "root_domain": result.candidate.root_domain,
            "harmonic_rank": result.candidate.harmonic_rank,
        },
        "outcome": result.outcome,
        "requested_url": result.requested_url,
        "final_url": result.final_url,
        "final_hostname": _hostname(result.final_url),
        "http_fallback_used": result.http_fallback_used,
        "scanned_at": result.scanned_at.isoformat(),
        "duration_ms": result.duration_ms,
        "error_message": result.error_message,
        "report": (
            result.report.model_dump(mode="json") if result.report is not None else None
        ),
    }


def _clickhouse_row(
    stored: StoredWebtechResult,
    *,
    crawl_id: str,
    partition_key: str,
    run_id: str,
    recorded_at: datetime,
) -> tuple[object, ...]:
    result = stored.result
    return (
        crawl_id,
        result.candidate.root_domain,
        result.candidate.harmonic_rank,
        WEBTECH_DETECTOR_VERSION,
        partition_key,
        run_id,
        result.outcome,
        result.requested_url,
        result.final_url,
        _hostname(result.final_url),
        int(result.http_fallback_used),
        len(result.report.technologies) if result.report is not None else 0,
        stored.bucket,
        stored.object_key,
        stored.sha256,
        stored.size_bytes,
        result.scanned_at,
        result.duration_ms,
        result.error_message[:2_000],
        recorded_at,
    )


def _hostname(url: str) -> str:
    if url == "":
        return ""
    return urlsplit(url).hostname or ""


def _validate_crawl_id(crawl_id: str) -> None:
    if re.fullmatch(r"CC-MAIN-[A-Za-z0-9][A-Za-z0-9._-]{0,127}", crawl_id) is None:
        raise ValueError(f"Invalid Common Crawl ID: {crawl_id!r}")


def _validate_root_domain(root_domain: str) -> None:
    if (
        root_domain == ""
        or root_domain != root_domain.strip().lower()
        or "/" in root_domain
        or ".." in root_domain
    ):
        raise ValueError(f"Invalid root domain: {root_domain!r}")
