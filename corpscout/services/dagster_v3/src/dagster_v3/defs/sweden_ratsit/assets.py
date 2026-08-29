import gzip
import hashlib
import json
import re
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_ratsit.normalization import (
    RATSIT_COMPANY_INDUSTRY_CODES_TABLE,
    RATSIT_COMPANY_SUMMARIES_TABLE,
    RATSIT_COMPANY_TABLE,
    RATSIT_ESTABLISHMENTS_TABLE,
    RATSIT_FINANCIAL_PERIODS_TABLE,
    RATSIT_FINANCIAL_REPORTS_TABLE,
    RATSIT_NACE_REVISION,
    RATSIT_NORMALIZATION_STATISTIC_KEYS,
    RATSIT_NORMALIZED_TABLES,
    RATSIT_NORMALIZER_VERSION,
    RATSIT_RESPONSIBLE_PEOPLE_TABLE,
    insert_normalized_ratsit_reports,
    load_nace_rev_2_1_class_codes,
    normalize_ratsit_report,
    select_latest_unnormalized_ratsit_reports,
)
from dagster_v3.defs.sweden_ratsit.resources import (
    RATSIT_BROWSER_WORKER_COUNT,
    RATSIT_HARD_MAX_COMPANIES,
    RatsitConnectionMode,
    RatsitCompanyFailure,
    RatsitCompanyNotFound,
    RatsitCompanyReport,
    RatsitProxyName,
    SwedenRatsitBrowserResource,
    ratsit_round_robin_assignments,
    validate_ratsit_company_report,
)

RATSIT_MAX_COMPANIES = RATSIT_HARD_MAX_COMPANIES
RATSIT_S3_BUCKET = "source-sweden-ratsit"
RATSIT_S3_PREFIX = "sweden_ratsit/pilot"
RATSIT_SCHEMA_VERSION = 1
RATSIT_PARSER_VERSION = "ratsit-html-v1"
RATSIT_BROWSER_POOL = "sweden_ratsit_browser"
RATSIT_NORMALIZE_POOL = "sweden_ratsit_normalize"
RATSIT_CLICKHOUSE_DATABASE = "corpscout"
RATSIT_ACTIVE_COMPANIES_TABLE = "se_companies"
RATSIT_RESULT_TABLE = "se_company_ratsit"
RATSIT_BUCKET_COUNT = 128
RATSIT_PROGRESS_LOG_EVERY_RESULTS = 25
RATSIT_PROGRESS_LOG_EVERY_SECONDS = 30.0
RATSIT_SUCCESS_FRESHNESS = timedelta(days=30)
RATSIT_S3_FRESHNESS_WORKER_COUNT = 16
RATSIT_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket_index:03d}" for bucket_index in range(RATSIT_BUCKET_COUNT)]
)

RATSIT_RESULT_COLUMNS = (
    "scan_id",
    "company_id",
    "outcome",
    "failure_type",
    "connection_mode",
    "proxy_name",
    "requested_url",
    "source_url",
    "http_status",
    "result_bucket",
    "result_object_key",
    "result_sha256",
    "result_size_bytes",
    "report_reused",
    "source_html_sha256",
    "diagnostic_object_key",
    "schema_version",
    "parser_version",
    "fetched_at",
    "error_message",
    "recorded_at",
)
type RatsitResultFilename = Literal[
    "report.json",
    "error.json",
    "not_found.json",
    "diagnostic.html.gz",
]


@dataclass(frozen=True)
class StoredRatsitReport:
    company_id: str
    result_sha256: str
    result_bucket: str
    result_object_key: str
    result_size_bytes: int


@dataclass(frozen=True)
class RatsitScanSelection:
    active_company_ids: tuple[str, ...]
    clickhouse_fresh_company_ids: tuple[str, ...]
    s3_fresh_company_ids: tuple[str, ...]
    selected_company_ids: tuple[str, ...]
    freshness_cutoff: datetime


@dataclass(frozen=True)
class RatsitScanResult:
    scan_id: str
    company_id: str
    outcome: str
    failure_type: str
    connection_mode: RatsitConnectionMode
    proxy_name: RatsitProxyName
    requested_url: str
    source_url: str
    http_status: int | None
    result_bucket: str
    result_object_key: str
    result_sha256: str
    result_size_bytes: int
    report_reused: bool
    source_html_sha256: str | None
    diagnostic_object_key: str
    schema_version: int
    parser_version: str
    fetched_at: datetime
    error_message: str


@dataclass(frozen=True)
class RatsitScanSummary:
    scan_id: str
    selected_company_ids: tuple[str, ...]
    started_at: datetime
    completed_at: datetime
    results: tuple[RatsitScanResult, ...]
    success_count: int
    not_found_count: int
    failure_count: int
    reused_report_count: int
    diagnostic_html_count: int
    written_object_count: int

    @property
    def result_object_keys(self) -> tuple[str, ...]:
        return tuple(result.result_object_key for result in self.results)


@dataclass(frozen=True)
class RatsitScanProgress:
    total_count: int
    completed_count: int
    success_count: int
    not_found_count: int
    failure_count: int
    reused_report_count: int
    latest_result: RatsitScanResult


type RatsitScanProgressCallback = Callable[[RatsitScanProgress], None]


def ratsit_bucket_key(company_id: str) -> str:
    _validate_ratsit_company_id(company_id)
    bucket_index = zlib.crc32(company_id.encode("ascii")) % RATSIT_BUCKET_COUNT
    return f"bucket_{bucket_index:03d}"


def load_active_ratsit_company_ids(
    clickhouse: ClickhouseResource,
    partition_key: str,
) -> tuple[str, ...]:
    bucket_index = _ratsit_bucket_index(partition_key)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_ACTIVE_COMPANIES_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT company_id
            FROM {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_ACTIVE_COMPANIES_TABLE} FINAL
            WHERE status = 'active'
              AND modulo(CRC32(company_id), %(bucket_count)s) = %(bucket_index)s
            ORDER BY company_id
            """,
            {
                "bucket_count": RATSIT_BUCKET_COUNT,
                "bucket_index": bucket_index,
            },
        )

    company_ids = tuple(str(row[0]) for row in rows)
    if not company_ids:
        raise ValueError(f"Ratsit partition {partition_key} has no active companies")
    if len(company_ids) > RATSIT_MAX_COMPANIES:
        raise ValueError(
            f"Ratsit partition {partition_key} contains {len(company_ids)} companies; "
            f"the safety limit is {RATSIT_MAX_COMPANIES}"
        )
    if len(set(company_ids)) != len(company_ids):
        raise ValueError(
            f"Ratsit partition {partition_key} contains duplicate company IDs"
        )
    for company_id in company_ids:
        _validate_ratsit_company_id(company_id)
        if ratsit_bucket_key(company_id) != partition_key:
            raise ValueError(
                f"Ratsit company {company_id} does not belong to {partition_key}"
            )
    return company_ids


def load_fresh_ratsit_company_ids_from_clickhouse(
    clickhouse: ClickhouseResource,
    company_ids: tuple[str, ...],
    freshness_cutoff: datetime,
) -> frozenset[str]:
    _require_aware_timestamp(freshness_cutoff, label="freshness cutoff")
    if not company_ids:
        return frozenset()

    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_RESULT_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT DISTINCT company_id
            FROM {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE} FINAL
            WHERE outcome = 'success'
              AND http_status = 200
              AND fetched_at >= %(freshness_cutoff)s
              AND company_id IN %(company_ids)s
            """,
            {
                "company_ids": company_ids,
                "freshness_cutoff": freshness_cutoff,
            },
        )

    fresh_company_ids = frozenset(str(row[0]) for row in rows)
    unexpected_company_ids = fresh_company_ids.difference(company_ids)
    if unexpected_company_ids:
        raise RuntimeError(
            "ClickHouse returned unselected Ratsit companies: "
            f"{', '.join(sorted(unexpected_company_ids)[:5])}"
        )
    return fresh_company_ids


def load_fresh_ratsit_company_ids_from_s3(
    object_store: ObjectStoreResource,
    company_ids: tuple[str, ...],
    freshness_cutoff: datetime,
) -> frozenset[str]:
    _require_aware_timestamp(freshness_cutoff, label="freshness cutoff")
    if not company_ids:
        return frozenset()

    object_store.client()
    worker_count = min(RATSIT_S3_FRESHNESS_WORKER_COUNT, len(company_ids))
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="ratsit_s3_freshness",
    ) as executor:
        freshness_results = executor.map(
            lambda company_id: _has_fresh_ratsit_report_in_s3(
                object_store,
                company_id=company_id,
                freshness_cutoff=freshness_cutoff,
            ),
            company_ids,
        )
        return frozenset(
            company_id
            for company_id, is_fresh in zip(
                company_ids,
                freshness_results,
                strict=True,
            )
            if is_fresh
        )


def select_ratsit_companies_for_scan(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    active_company_ids: tuple[str, ...],
    freshness_cutoff: datetime,
) -> RatsitScanSelection:
    clickhouse_fresh_company_ids = load_fresh_ratsit_company_ids_from_clickhouse(
        clickhouse,
        active_company_ids,
        freshness_cutoff,
    )
    s3_candidates = tuple(
        company_id
        for company_id in active_company_ids
        if company_id not in clickhouse_fresh_company_ids
    )
    s3_fresh_company_ids = load_fresh_ratsit_company_ids_from_s3(
        object_store,
        s3_candidates,
        freshness_cutoff,
    )
    selected_company_ids = tuple(
        company_id
        for company_id in s3_candidates
        if company_id not in s3_fresh_company_ids
    )
    return RatsitScanSelection(
        active_company_ids=active_company_ids,
        clickhouse_fresh_company_ids=tuple(
            company_id
            for company_id in active_company_ids
            if company_id in clickhouse_fresh_company_ids
        ),
        s3_fresh_company_ids=tuple(
            company_id
            for company_id in active_company_ids
            if company_id in s3_fresh_company_ids
        ),
        selected_company_ids=selected_company_ids,
        freshness_cutoff=freshness_cutoff,
    )


def ratsit_result_object_key(
    company_id: str,
    scan_id: str,
    filename: RatsitResultFilename,
) -> str:
    _validate_scan_id(scan_id)
    stem, extension = filename.split(".", maxsplit=1)
    return f"{_company_prefix(company_id)}/{scan_id}_{stem}.{extension}"


def load_reusable_ratsit_reports(
    clickhouse: ClickhouseResource,
    company_ids: tuple[str, ...],
) -> dict[tuple[str, str], StoredRatsitReport]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_RESULT_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT
                company_id,
                toString(result_sha256),
                argMax(
                    tuple(result_bucket, result_object_key, result_size_bytes),
                    tuple(fetched_at, recorded_at, scan_id)
                )
            FROM {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE} FINAL
            WHERE outcome = 'success'
              AND company_id IN %(company_ids)s
            GROUP BY company_id, result_sha256
            """,
            {"company_ids": company_ids},
        )

    reusable: dict[tuple[str, str], StoredRatsitReport] = {}
    for company_id, result_sha256, location in rows:
        result_bucket, result_object_key, result_size_bytes = location
        stored = StoredRatsitReport(
            company_id=str(company_id),
            result_sha256=str(result_sha256),
            result_bucket=str(result_bucket),
            result_object_key=str(result_object_key),
            result_size_bytes=int(result_size_bytes),
        )
        reusable[(stored.company_id, stored.result_sha256)] = stored
    return reusable


def write_ratsit_scan(
    *,
    object_store: ObjectStoreResource,
    ratsit: SwedenRatsitBrowserResource,
    company_ids: tuple[str, ...],
    scan_id: str,
    on_progress: RatsitScanProgressCallback,
    reusable_reports: Mapping[tuple[str, str], StoredRatsitReport] | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> RatsitScanSummary:
    _validate_scan_id(scan_id)
    if len(company_ids) > RATSIT_MAX_COMPANIES:
        raise ValueError(f"Ratsit accepts at most {RATSIT_MAX_COMPANIES} companies")

    scan_started_at = started_at or datetime.now(UTC)
    _require_aware_timestamp(scan_started_at, label="scan start")
    selected_company_ids = set(company_ids)
    if len(selected_company_ids) != len(company_ids):
        raise ValueError("Ratsit company IDs must be unique")

    if company_ids:
        object_store.ensure_bucket(bucket=RATSIT_S3_BUCKET)
    stored_reports = reusable_reports or {}
    scan_results: list[RatsitScanResult] = []
    resolved_company_ids: set[str] = set()
    success_count = 0
    not_found_count = 0
    failure_count = 0
    reused_report_count = 0
    diagnostic_html_count = 0
    written_object_count = 0

    def report_progress(latest_result: RatsitScanResult) -> None:
        on_progress(
            RatsitScanProgress(
                total_count=len(company_ids),
                completed_count=len(scan_results),
                success_count=success_count,
                not_found_count=not_found_count,
                failure_count=failure_count,
                reused_report_count=reused_report_count,
                latest_result=latest_result,
            )
        )

    for result in ratsit.iter_company_reports(company_ids):
        if result.company_id not in selected_company_ids:
            raise RuntimeError(
                f"Ratsit browser returned unselected company {result.company_id}"
            )
        if result.company_id in resolved_company_ids:
            raise RuntimeError(
                f"Ratsit browser returned company {result.company_id} more than once"
            )
        resolved_company_ids.add(result.company_id)
        if isinstance(result, RatsitCompanyReport):
            report_json = _report_json(result)
            report_sha256 = _sha256(report_json)
            reusable = stored_reports.get((result.company_id, report_sha256))
            if reusable is None:
                result_bucket = RATSIT_S3_BUCKET
                result_object_key = ratsit_result_object_key(
                    result.company_id,
                    scan_id,
                    "report.json",
                )
                result_size_bytes = len(report_json.encode("utf-8"))
                object_store.write_json(
                    result_object_key,
                    report_json,
                    bucket=result_bucket,
                )
                report_reused = False
                written_object_count += 1
            else:
                result_bucket = reusable.result_bucket
                result_object_key = reusable.result_object_key
                result_size_bytes = reusable.result_size_bytes
                report_reused = True
                reused_report_count += 1

            scan_results.append(
                RatsitScanResult(
                    scan_id=scan_id,
                    company_id=result.company_id,
                    outcome="success",
                    failure_type="",
                    connection_mode=result.connection_mode,
                    proxy_name=result.proxy_name,
                    requested_url=result.requested_url,
                    source_url=result.source_url,
                    http_status=result.http_status,
                    result_bucket=result_bucket,
                    result_object_key=result_object_key,
                    result_sha256=report_sha256,
                    result_size_bytes=result_size_bytes,
                    report_reused=report_reused,
                    source_html_sha256=result.html_sha256,
                    diagnostic_object_key="",
                    schema_version=RATSIT_SCHEMA_VERSION,
                    parser_version=RATSIT_PARSER_VERSION,
                    fetched_at=result.fetched_at,
                    error_message="",
                )
            )
            success_count += 1
            report_progress(scan_results[-1])
            continue

        if isinstance(result, RatsitCompanyNotFound):
            result_json = _not_found_json(result, scan_id=scan_id)
            result_key = ratsit_result_object_key(
                result.company_id,
                scan_id,
                "not_found.json",
            )
            outcome = "not_found"
            failure_type = ""
            error_message = ""
            not_found_count += 1
        else:
            result_json = _error_json(result, scan_id=scan_id)
            result_key = ratsit_result_object_key(
                result.company_id,
                scan_id,
                "error.json",
            )
            outcome = "failure"
            failure_type = result.error_type
            error_message = result.message
            failure_count += 1

        object_store.write_json(result_key, result_json, bucket=RATSIT_S3_BUCKET)
        written_object_count += 1
        diagnostic_object_key = ""
        if result.diagnostic_html is not None:
            diagnostic_object_key = ratsit_result_object_key(
                result.company_id,
                scan_id,
                "diagnostic.html.gz",
            )
            object_store.write_bytes(
                diagnostic_object_key,
                gzip.compress(result.diagnostic_html, compresslevel=9, mtime=0),
                bucket=RATSIT_S3_BUCKET,
            )
            diagnostic_html_count += 1
            written_object_count += 1

        scan_results.append(
            RatsitScanResult(
                scan_id=scan_id,
                company_id=result.company_id,
                outcome=outcome,
                failure_type=failure_type,
                connection_mode=result.connection_mode,
                proxy_name=result.proxy_name,
                requested_url=result.requested_url,
                source_url=result.source_url,
                http_status=result.http_status,
                result_bucket=RATSIT_S3_BUCKET,
                result_object_key=result_key,
                result_sha256=_sha256(result_json),
                result_size_bytes=len(result_json.encode("utf-8")),
                report_reused=False,
                source_html_sha256=result.html_sha256,
                diagnostic_object_key=diagnostic_object_key,
                schema_version=RATSIT_SCHEMA_VERSION,
                parser_version=RATSIT_PARSER_VERSION,
                fetched_at=result.fetched_at,
                error_message=error_message,
            )
        )
        report_progress(scan_results[-1])

    if resolved_company_ids != selected_company_ids:
        missing_company_ids = sorted(selected_company_ids - resolved_company_ids)
        raise RuntimeError(
            "Ratsit browser did not return every selected company; missing "
            f"{', '.join(missing_company_ids[:5])}"
        )
    scan_results_by_company = {result.company_id: result for result in scan_results}
    ordered_scan_results = tuple(
        scan_results_by_company[company_id] for company_id in company_ids
    )

    scan_completed_at = completed_at or datetime.now(UTC)
    _require_aware_timestamp(scan_completed_at, label="scan completion")
    if scan_completed_at < scan_started_at:
        raise ValueError("Ratsit scan cannot complete before it starts")
    if any(result.fetched_at > scan_completed_at for result in ordered_scan_results):
        raise ValueError("Ratsit scan cannot complete before a company was fetched")

    return RatsitScanSummary(
        scan_id=scan_id,
        selected_company_ids=company_ids,
        started_at=scan_started_at,
        completed_at=scan_completed_at,
        results=ordered_scan_results,
        success_count=success_count,
        not_found_count=not_found_count,
        failure_count=failure_count,
        reused_report_count=reused_report_count,
        diagnostic_html_count=diagnostic_html_count,
        written_object_count=written_object_count,
    )


def persist_ratsit_scan(
    clickhouse: ClickhouseResource,
    summary: RatsitScanSummary,
    *,
    recorded_at: datetime | None = None,
) -> int:
    indexed_at = recorded_at or datetime.now(UTC)
    _require_aware_timestamp(indexed_at, label="scan index")
    if summary.completed_at > indexed_at:
        raise ValueError("Ratsit scan cannot be indexed before it completed")
    if len(summary.results) != len(summary.selected_company_ids):
        raise ValueError("A completed Ratsit scan must have one result per company")
    if not summary.results:
        return 0

    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_RESULT_TABLE,),
    )

    result_rows = [
        (
            result.scan_id,
            result.company_id,
            result.outcome,
            result.failure_type,
            result.connection_mode,
            result.proxy_name,
            result.requested_url,
            result.source_url,
            result.http_status,
            result.result_bucket,
            result.result_object_key,
            result.result_sha256,
            result.result_size_bytes,
            int(result.report_reused),
            result.source_html_sha256,
            result.diagnostic_object_key,
            result.schema_version,
            result.parser_version,
            result.fetched_at,
            result.error_message,
            indexed_at,
        )
        for result in summary.results
    ]
    with clickhouse.get_connection() as client:
        client.execute(
            f"""
            INSERT INTO {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE}
            ({", ".join(RATSIT_RESULT_COLUMNS)}) VALUES
            """,
            result_rows,
        )
    return len(result_rows)


def _company_prefix(company_id: str) -> str:
    _validate_ratsit_company_id(company_id)
    return f"{RATSIT_S3_PREFIX}/company_id={company_id}"


def _has_fresh_ratsit_report_in_s3(
    object_store: ObjectStoreResource,
    *,
    company_id: str,
    freshness_cutoff: datetime,
) -> bool:
    company_prefix = f"{_company_prefix(company_id)}/"
    recent_report_objects = []
    for stored_object in object_store.list_objects(
        company_prefix,
        bucket=RATSIT_S3_BUCKET,
    ):
        _require_aware_timestamp(
            stored_object.last_modified,
            label="S3 LastModified",
        )
        if not _is_ratsit_report_object_key(
            stored_object.key,
            company_prefix=company_prefix,
        ):
            continue
        if stored_object.last_modified.astimezone(UTC) < freshness_cutoff:
            continue
        recent_report_objects.append(stored_object)

    for stored_object in sorted(
        recent_report_objects,
        key=lambda item: item.last_modified,
        reverse=True,
    ):
        report_bytes = object_store.read_bytes(
            stored_object.key,
            bucket=RATSIT_S3_BUCKET,
        )
        try:
            report_document = json.loads(report_bytes)
        except json.JSONDecodeError, UnicodeDecodeError:
            continue
        if _is_valid_stored_ratsit_report(
            report_document,
            expected_company_id=company_id,
        ):
            return True
    return False


def _is_ratsit_report_object_key(key: str, *, company_prefix: str) -> bool:
    if not key.startswith(company_prefix):
        return False
    object_name = key.removeprefix(company_prefix)
    return (
        re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}_report\.json",
            object_name,
        )
        is not None
    )


def _is_valid_stored_ratsit_report(
    document: object,
    *,
    expected_company_id: str,
) -> bool:
    if not isinstance(document, Mapping):
        return False
    if document.get("schema_version") != RATSIT_SCHEMA_VERSION:
        return False
    if document.get("parser_version") != RATSIT_PARSER_VERSION:
        return False
    if document.get("company_id") != expected_company_id:
        return False
    if not isinstance(document.get("requested_url"), str):
        return False
    if not isinstance(document.get("source_url"), str):
        return False
    report = document.get("report")
    if not isinstance(report, Mapping):
        return False
    try:
        validate_ratsit_company_report(
            report,
            expected_company_id=expected_company_id,
        )
    except ValueError:
        return False
    return True


def _validate_ratsit_company_id(company_id: str) -> None:
    if re.fullmatch(r"(?:[0-9]{10}|[0-9]{12})", company_id) is None:
        raise ValueError("Ratsit company ID must contain ten or twelve digits")


def _ratsit_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid Ratsit partition: {partition_key!r}")
    bucket_index = int(suffix)
    if not 0 <= bucket_index < RATSIT_BUCKET_COUNT:
        raise ValueError(f"Ratsit partition is out of range: {partition_key}")
    if partition_key != f"bucket_{bucket_index:03d}":
        raise ValueError(f"Invalid Ratsit partition: {partition_key!r}")
    return bucket_index


def _validate_scan_id(scan_id: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", scan_id) is None:
        raise ValueError("Ratsit scan ID is not safe for an S3 object name")


def _report_json(report: RatsitCompanyReport) -> str:
    return _json_document(
        {
            "schema_version": RATSIT_SCHEMA_VERSION,
            "parser_version": RATSIT_PARSER_VERSION,
            "company_id": report.company_id,
            "requested_url": report.requested_url,
            "source_url": report.source_url,
            "report": report.report,
        }
    )


def _error_json(failure: RatsitCompanyFailure, *, scan_id: str) -> str:
    return _json_document(
        {
            "schema_version": RATSIT_SCHEMA_VERSION,
            "parser_version": RATSIT_PARSER_VERSION,
            "scan_id": scan_id,
            "company_id": failure.company_id,
            "requested_url": failure.requested_url,
            "source_url": failure.source_url,
            "fetched_at": failure.fetched_at.isoformat(),
            "error_type": failure.error_type,
            "message": failure.message,
            "http_status": failure.http_status,
            "html_sha256": failure.html_sha256,
        }
    )


def _not_found_json(result: RatsitCompanyNotFound, *, scan_id: str) -> str:
    return _json_document(
        {
            "schema_version": RATSIT_SCHEMA_VERSION,
            "parser_version": RATSIT_PARSER_VERSION,
            "scan_id": scan_id,
            "company_id": result.company_id,
            "requested_url": result.requested_url,
            "source_url": result.source_url,
            "fetched_at": result.fetched_at.isoformat(),
            "outcome": "not_found",
            "reason": result.reason,
            "message": result.message,
            "http_status": result.http_status,
            "html_sha256": result.html_sha256,
        }
    )


def _json_document(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require_aware_timestamp(value: datetime, *, label: str) -> None:
    if value.utcoffset() is None:
        raise ValueError(f"Ratsit {label} timestamp must include a timezone")


@dg.asset(
    deps=[dg.AssetKey("sweden_company_companies_clickhouse")],
    group_name="sweden_ratsit",
    kinds={"python", "browser", "html", "json", "s3", "clickhouse", "ratsit"},
    tags={
        "country": "sweden",
        "source": "ratsit",
        "source_name": "sweden_ratsit",
        "entity_type": "company",
        "layer": "scan_dispatch",
    },
    partitions_def=RATSIT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=RATSIT_BROWSER_POOL,
    description=(
        "Selects one of 128 stable CRC32 buckets from active corpscout.se_companies, "
        "skips companies successfully fetched in the previous 30 days using "
        "ClickHouse fetched_at or an existing valid S3 report's LastModified, "
        "then renders and parses its Ratsit pages with four parallel headless "
        "CloakBrowsers: one direct and three proxied. Each browser spaces request "
        "starts by at least two seconds. Every company outcome is indexed by "
        "Dagster run ID in ClickHouse. Changed reports are written to per-company "
        "run-ID keys; identical report hashes reuse the prior S3 object. A bucket "
        "with any HTTP 429 outcomes is marked failed only after all selected "
        "companies and result writes are complete."
    ),
)
def se_ratsit_scan_dispatch(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_ratsit_browser: SwedenRatsitBrowserResource,
    sweden_ratsit_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    scan_id = context.run.run_id
    started_at = datetime.now(UTC)
    active_company_ids = load_active_ratsit_company_ids(clickhouse, partition_key)
    freshness_cutoff = started_at - RATSIT_SUCCESS_FRESHNESS
    context.log.info(
        "Selecting Ratsit companies: scan_id=%s partition=%s active_companies=%s "
        "freshness_cutoff=%s",
        scan_id,
        partition_key,
        len(active_company_ids),
        freshness_cutoff.isoformat(),
    )
    selection = select_ratsit_companies_for_scan(
        clickhouse=clickhouse,
        object_store=sweden_ratsit_object_store,
        active_company_ids=active_company_ids,
        freshness_cutoff=freshness_cutoff,
    )
    company_ids = selection.selected_company_ids
    context.log.info(
        "Starting Ratsit scan: scan_id=%s partition=%s active_companies=%s "
        "skipped_clickhouse=%s skipped_s3=%s selected_companies=%s "
        "browser_workers=%s request_interval_seconds=%s",
        scan_id,
        partition_key,
        len(active_company_ids),
        len(selection.clickhouse_fresh_company_ids),
        len(selection.s3_fresh_company_ids),
        len(company_ids),
        RATSIT_BROWSER_WORKER_COUNT,
        sweden_ratsit_browser.request_interval_seconds,
    )
    reusable_reports = (
        load_reusable_ratsit_reports(
            clickhouse,
            company_ids,
        )
        if company_ids
        else {}
    )
    progress_started_at = time.monotonic()
    last_progress_logged_at = progress_started_at

    def log_progress(progress: RatsitScanProgress) -> None:
        nonlocal last_progress_logged_at

        logged_at = time.monotonic()
        should_log = (
            progress.completed_count == 1
            or progress.completed_count == progress.total_count
            or progress.completed_count % RATSIT_PROGRESS_LOG_EVERY_RESULTS == 0
            or logged_at - last_progress_logged_at >= RATSIT_PROGRESS_LOG_EVERY_SECONDS
        )
        if not should_log:
            return

        elapsed_seconds = max(logged_at - progress_started_at, 0.001)
        companies_per_minute = progress.completed_count / elapsed_seconds * 60
        remaining_count = progress.total_count - progress.completed_count
        eta_seconds = remaining_count / (progress.completed_count / elapsed_seconds)
        latest_result = progress.latest_result
        context.log.info(
            "Ratsit scan progress: partition=%s completed=%s/%s percent=%.1f "
            "success=%s not_found=%s failure=%s reused=%s "
            "rate_companies_per_minute=%.1f elapsed_seconds=%s eta_seconds=%s "
            "last_company_id=%s last_outcome=%s last_route=%s "
            "last_http_status=%s last_failure_type=%s",
            partition_key,
            progress.completed_count,
            progress.total_count,
            progress.completed_count / progress.total_count * 100,
            progress.success_count,
            progress.not_found_count,
            progress.failure_count,
            progress.reused_report_count,
            companies_per_minute,
            round(elapsed_seconds),
            round(eta_seconds),
            latest_result.company_id,
            latest_result.outcome,
            latest_result.proxy_name or "direct",
            latest_result.http_status,
            latest_result.failure_type,
        )
        last_progress_logged_at = logged_at

    summary = write_ratsit_scan(
        object_store=sweden_ratsit_object_store,
        ratsit=sweden_ratsit_browser,
        company_ids=company_ids,
        scan_id=scan_id,
        on_progress=log_progress,
        reusable_reports=reusable_reports,
        started_at=started_at,
    )
    indexed_result_count = persist_ratsit_scan(clickhouse, summary)
    browser_assignments = ratsit_round_robin_assignments(summary.selected_company_ids)
    http_429_counts_by_route = {
        worker_name: sum(
            1
            for result in summary.results
            if result.http_status == 429
            and (result.proxy_name if result.proxy_name else "direct") == worker_name
        )
        for worker_name, _ in browser_assignments
    }
    http_429_count = sum(http_429_counts_by_route.values())

    context.log.info(
        "Finished Ratsit scan: scan_id=%s successes=%s not_found=%s failures=%s "
        "reused=%s objects_written=%s http_429s=%s",
        scan_id,
        summary.success_count,
        summary.not_found_count,
        summary.failure_count,
        summary.reused_report_count,
        summary.written_object_count,
        http_429_count,
    )
    materialization_metadata = {
        "scan_id": scan_id,
        "partition_key": partition_key,
        "hash_algorithm": "CRC32",
        "hash_bucket_count": RATSIT_BUCKET_COUNT,
        "active_company_count": len(selection.active_company_ids),
        "freshness_window_days": RATSIT_SUCCESS_FRESHNESS.days,
        "freshness_cutoff": selection.freshness_cutoff.isoformat(),
        "skipped_recent_clickhouse_count": len(selection.clickhouse_fresh_company_ids),
        "skipped_recent_s3_count": len(selection.s3_fresh_company_ids),
        "skipped_recent_total_count": (
            len(selection.clickhouse_fresh_company_ids)
            + len(selection.s3_fresh_company_ids)
        ),
        "selected_company_count": len(summary.selected_company_ids),
        "first_company_id": (
            summary.selected_company_ids[0] if summary.selected_company_ids else ""
        ),
        "last_company_id": (
            summary.selected_company_ids[-1] if summary.selected_company_ids else ""
        ),
        "success_count": summary.success_count,
        "not_found_count": summary.not_found_count,
        "failure_count": summary.failure_count,
        "reused_report_count": summary.reused_report_count,
        "diagnostic_html_count": summary.diagnostic_html_count,
        "written_object_count": summary.written_object_count,
        "indexed_result_count": indexed_result_count,
        "browser_assignment_counts": {
            worker_name: len(assigned_company_ids)
            for worker_name, assigned_company_ids in browser_assignments
        },
        "browser_worker_count": RATSIT_BROWSER_WORKER_COUNT,
        "proxy_browser_count": RATSIT_BROWSER_WORKER_COUNT - 1,
        "http_429_count": http_429_count,
        "http_429_counts_by_route": http_429_counts_by_route,
        "result_table": (f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE}"),
        "headless": sweden_ratsit_browser.headless,
        "request_interval_seconds": (sweden_ratsit_browser.request_interval_seconds),
        "effective_average_request_interval_seconds": (
            sweden_ratsit_browser.request_interval_seconds / RATSIT_BROWSER_WORKER_COUNT
        ),
        "schema_version": RATSIT_SCHEMA_VERSION,
        "parser_version": RATSIT_PARSER_VERSION,
        "s3_bucket": RATSIT_S3_BUCKET,
        "s3_prefix": f"{RATSIT_S3_PREFIX}/",
        "bucket_status": "failed_http_429" if http_429_count > 0 else "success",
    }
    if http_429_count > 0:
        context.instance.add_run_tags(scan_id, {"dagster/max_retries": "0"})
        raise dg.Failure(
            description=(
                f"Ratsit bucket {partition_key} completed with {http_429_count} "
                "HTTP 429 outcomes"
            ),
            metadata=materialization_metadata,
            allow_retries=False,
        )
    return dg.MaterializeResult(metadata=materialization_metadata)


RATSIT_NORMALIZATION_DEPS = (
    dg.AssetKey("se_ratsit_scan_dispatch"),
    dg.AssetKey("nace_categories_clickhouse"),
)


@dg.multi_asset(
    name="se_ratsit_normalized",
    specs=[
        dg.AssetSpec(
            RATSIT_COMPANY_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "company",
                "layer": "normalized",
            },
            metadata={"table": f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_COMPANY_TABLE}"},
            description=(
                "Latest successful Ratsit company report content normalized from "
                "its exact S3 JSON object. This row is the completion marker for "
                "all child segments of the same report hash."
            ),
        ),
        dg.AssetSpec(
            RATSIT_COMPANY_INDUSTRY_CODES_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "company_industry",
                "layer": "normalized",
            },
            metadata={
                "table": (
                    f"{RATSIT_CLICKHOUSE_DATABASE}."
                    f"{RATSIT_COMPANY_INDUSTRY_CODES_TABLE}"
                )
            },
            description="Ratsit company industry-code observations from S3 JSON.",
        ),
        dg.AssetSpec(
            RATSIT_COMPANY_SUMMARIES_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "company_summary",
                "layer": "normalized",
            },
            metadata={
                "table": (
                    f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_COMPANY_SUMMARIES_TABLE}"
                )
            },
            description="Ordered Ratsit company-summary paragraphs from S3 JSON.",
        ),
        dg.AssetSpec(
            RATSIT_RESPONSIBLE_PEOPLE_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "company_person_observation",
                "layer": "normalized",
                "contains_personal_data": "true",
            },
            metadata={
                "table": (
                    f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESPONSIBLE_PEOPLE_TABLE}"
                )
            },
            description=(
                "Source-level Ratsit responsible-person and role observations."
            ),
        ),
        dg.AssetSpec(
            RATSIT_ESTABLISHMENTS_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "establishment",
                "layer": "normalized",
            },
            metadata={
                "table": (f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_ESTABLISHMENTS_TABLE}")
            },
            description=(
                "Ratsit workplaces normalized as physical company establishments."
            ),
        ),
        dg.AssetSpec(
            RATSIT_FINANCIAL_REPORTS_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "financial_report",
                "layer": "normalized",
            },
            metadata={
                "table": (
                    f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_FINANCIAL_REPORTS_TABLE}"
                )
            },
            description="Ratsit financial report scopes and monetary units.",
        ),
        dg.AssetSpec(
            RATSIT_FINANCIAL_PERIODS_TABLE,
            deps=RATSIT_NORMALIZATION_DEPS,
            group_name="sweden_ratsit",
            kinds={"python", "json", "s3", "clickhouse", "ratsit"},
            tags={
                "country": "sweden",
                "source": "ratsit",
                "source_name": "sweden_ratsit",
                "entity_type": "financial_period",
                "layer": "normalized",
            },
            metadata={
                "table": (
                    f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_FINANCIAL_PERIODS_TABLE}"
                )
            },
            description=(
                "Ratsit income statement, balance sheet, and key-ratio values "
                "at financial-period grain."
            ),
        ),
    ],
    partitions_def=RATSIT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=RATSIT_NORMALIZE_POOL,
    can_subset=False,
)
def se_ratsit_normalized(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_ratsit_object_store: ObjectStoreResource,
) -> Iterator[dg.MaterializeResult]:
    """Normalize the latest successful report per company without launching browsers."""
    partition_key = context.partition_key
    bucket_index = _ratsit_bucket_index(partition_key)
    normalized_at = datetime.now(UTC)
    selection = select_latest_unnormalized_ratsit_reports(
        clickhouse,
        bucket_count=RATSIT_BUCKET_COUNT,
        bucket_index=bucket_index,
    )
    nace_class_codes = load_nace_rev_2_1_class_codes(clickhouse)
    context.log.info(
        "Starting Ratsit JSON normalization: partition=%s latest_successes=%s "
        "already_normalized=%s candidates=%s normalizer_version=%s",
        partition_key,
        selection.latest_success_count,
        selection.already_normalized_count,
        len(selection.reports),
        RATSIT_NORMALIZER_VERSION,
    )

    normalized_reports = []
    normalization_statistics: Counter[str] = Counter(
        {key: 0 for key in RATSIT_NORMALIZATION_STATISTIC_KEYS}
    )
    for report_index, report in enumerate(selection.reports, start=1):
        if (
            report_index == 1
            or report_index % 20 == 0
            or report_index == len(selection.reports)
        ):
            context.log.info(
                "Normalizing Ratsit S3 report %s/%s: company_id=%s object_key=%s",
                report_index,
                len(selection.reports),
                report.company_id,
                report.result_object_key,
            )
        document = sweden_ratsit_object_store.read_bytes(
            report.result_object_key,
            bucket=report.result_bucket,
        )
        normalized = normalize_ratsit_report(
            report,
            document=document,
            normalized_at=normalized_at,
            nace_class_codes=nace_class_codes,
        )
        normalized_reports.append(normalized)
        normalization_statistics.update(normalized.statistics)

    inserted_rows = insert_normalized_ratsit_reports(
        clickhouse,
        reports=tuple(normalized_reports),
    )
    context.log.info(
        "Finished Ratsit JSON normalization: candidates=%s inserted_rows=%s",
        len(normalized_reports),
        inserted_rows,
    )
    common_metadata = {
        "partition_key": partition_key,
        "hash_algorithm": "CRC32",
        "hash_bucket_count": RATSIT_BUCKET_COUNT,
        "latest_success_count": selection.latest_success_count,
        "already_normalized_count": selection.already_normalized_count,
        "normalized_report_count": len(normalized_reports),
        "normalizer_version": RATSIT_NORMALIZER_VERSION,
        "nace_revision": RATSIT_NACE_REVISION,
        "nace_class_reference_count": len(nace_class_codes),
        "normalized_at": normalized_at.isoformat(),
        "result_object_key_count": len(selection.reports),
        **normalization_statistics,
    }
    for table in RATSIT_NORMALIZED_TABLES:
        yield dg.MaterializeResult(
            asset_key=table,
            metadata={
                **common_metadata,
                "inserted_rows": inserted_rows[table],
                "table": f"{RATSIT_CLICKHOUSE_DATABASE}.{table}",
            },
        )


se_ratsit_scan_dispatch_job = dg.define_asset_job(
    name="se_ratsit_scan_dispatch_job",
    selection=dg.AssetSelection.assets(se_ratsit_scan_dispatch),
    description="Run one stable bucket of active Swedish companies through Ratsit.",
)

se_ratsit_normalize_job = dg.define_asset_job(
    name="se_ratsit_normalize_job",
    selection=dg.AssetSelection.assets(*RATSIT_NORMALIZED_TABLES),
    description=(
        "Normalize one bucket of companies' latest successful Ratsit S3 reports "
        "into source-specific ClickHouse tables."
    ),
)


defs = dg.Definitions(
    assets=[se_ratsit_scan_dispatch, se_ratsit_normalized],
    jobs=[se_ratsit_scan_dispatch_job, se_ratsit_normalize_job],
    resources={
        "sweden_ratsit_browser": SwedenRatsitBrowserResource(
            crawl_proxy1=dg.EnvVar("crawl_proxy1"),
            crawl_proxy2=dg.EnvVar("crawl_proxy2"),
            crawl_proxy3=dg.EnvVar("crawl_proxy3"),
        ),
        "sweden_ratsit_object_store": ObjectStoreResource(bucket=RATSIT_S3_BUCKET),
    },
)
