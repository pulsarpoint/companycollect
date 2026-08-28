import gzip
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_ratsit.resources import (
    RATSIT_HARD_MAX_COMPANIES,
    RatsitCompanyFailure,
    RatsitCompanyReport,
    SwedenRatsitBrowserResource,
)

RATSIT_COMPANY_IDS = (
    "5560004615",  # Skanska AB
    "5560125790",  # Aktiebolaget Volvo
    "5560160680",  # Telefonaktiebolaget LM Ericsson
    "5560094178",  # Aktiebolaget Electrolux
    "5560073495",  # Aktiebolaget SKF
    "5560593575",  # ASSA ABLOY AB
    "5560142720",  # Atlas Copco Aktiebolag
    "5560003468",  # Sandvik Aktiebolag
    "5560427220",  # H & M Hennes & Mauritz AB
    "5561034249",  # Telia Company AB
    "5560482837",  # ICA Gruppen Aktiebolag
    "5563027241",  # Securitas AB
    "5563255511",  # Essity Aktiebolag (publ)
    "5560514142",  # Boliden AB
    "5565878054",  # Alfa Laval AB
    "5560614330",  # Peab AB
    "5020329081",  # Skandinaviska Enskilda Banken AB
    "5560345174",  # NCC AKTIEBOLAG
    "5560360793",  # SAAB Aktiebolag
    "5560840976",  # Scania CV Aktiebolag
)
RATSIT_MAX_COMPANIES = RATSIT_HARD_MAX_COMPANIES
RATSIT_S3_BUCKET = "source-sweden-ratsit"
RATSIT_S3_PREFIX = "sweden_ratsit/pilot"
RATSIT_SCHEMA_VERSION = 1
RATSIT_PARSER_VERSION = "ratsit-html-v1"
RATSIT_BROWSER_POOL = "sweden_ratsit_browser"
RATSIT_CLICKHOUSE_DATABASE = "corpscout"
RATSIT_RESULT_TABLE = "se_company_ratsit_crawl_results"
RATSIT_SCAN_TABLE = "se_company_ratsit_scans"

RATSIT_RESULT_COLUMNS = (
    "scan_id",
    "company_id",
    "outcome",
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
RATSIT_SCAN_COLUMNS = (
    "scan_id",
    "status",
    "selected_company_ids",
    "selected_company_count",
    "result_count",
    "success_count",
    "failure_count",
    "reused_report_count",
    "written_object_count",
    "result_bucket",
    "result_prefix",
    "schema_version",
    "parser_version",
    "started_at",
    "completed_at",
    "error_type",
    "error_message",
    "recorded_at",
)

type RatsitResultFilename = Literal[
    "report.json",
    "error.json",
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
class RatsitScanResult:
    scan_id: str
    company_id: str
    outcome: str
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
    failure_count: int
    reused_report_count: int
    diagnostic_html_count: int
    written_object_count: int

    @property
    def result_object_keys(self) -> tuple[str, ...]:
        return tuple(result.result_object_key for result in self.results)


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
    reusable_reports: Mapping[tuple[str, str], StoredRatsitReport] | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> RatsitScanSummary:
    _validate_scan_id(scan_id)
    if len(company_ids) > RATSIT_MAX_COMPANIES:
        raise ValueError(f"Ratsit accepts at most {RATSIT_MAX_COMPANIES} companies")

    scan_started_at = started_at or datetime.now(UTC)
    _require_aware_timestamp(scan_started_at, label="scan start")
    results = tuple(ratsit.iter_company_reports(company_ids))
    resolved_company_ids = tuple(result.company_id for result in results)
    if resolved_company_ids != company_ids:
        raise RuntimeError(
            "Ratsit browser did not resolve each selected company exactly once"
        )

    object_store.ensure_bucket(bucket=RATSIT_S3_BUCKET)
    stored_reports = reusable_reports or {}
    scan_results: list[RatsitScanResult] = []
    success_count = 0
    failure_count = 0
    reused_report_count = 0
    diagnostic_html_count = 0
    written_object_count = 0

    for result in results:
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
            continue

        error_json = _error_json(result, scan_id=scan_id)
        error_key = ratsit_result_object_key(
            result.company_id,
            scan_id,
            "error.json",
        )
        object_store.write_json(error_key, error_json, bucket=RATSIT_S3_BUCKET)
        written_object_count += 1
        diagnostic_object_key = ""
        if result.error_type == "parse" and result.diagnostic_html is not None:
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
                outcome=result.error_type,
                requested_url=result.requested_url,
                source_url=result.source_url,
                http_status=result.http_status,
                result_bucket=RATSIT_S3_BUCKET,
                result_object_key=error_key,
                result_sha256=_sha256(error_json),
                result_size_bytes=len(error_json.encode("utf-8")),
                report_reused=False,
                source_html_sha256=result.html_sha256,
                diagnostic_object_key=diagnostic_object_key,
                schema_version=RATSIT_SCHEMA_VERSION,
                parser_version=RATSIT_PARSER_VERSION,
                fetched_at=result.fetched_at,
                error_message=result.message,
            )
        )
        failure_count += 1

    scan_completed_at = completed_at or datetime.now(UTC)
    _require_aware_timestamp(scan_completed_at, label="scan completion")
    if scan_completed_at < scan_started_at:
        raise ValueError("Ratsit scan cannot complete before it starts")
    if any(result.fetched_at > scan_completed_at for result in scan_results):
        raise ValueError("Ratsit scan cannot complete before a company was fetched")

    return RatsitScanSummary(
        scan_id=scan_id,
        selected_company_ids=company_ids,
        started_at=scan_started_at,
        completed_at=scan_completed_at,
        results=tuple(scan_results),
        success_count=success_count,
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
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_RESULT_TABLE, RATSIT_SCAN_TABLE),
    )
    indexed_at = recorded_at or datetime.now(UTC)
    _require_aware_timestamp(indexed_at, label="scan index")
    if summary.completed_at > indexed_at:
        raise ValueError("Ratsit scan cannot be indexed before it completed")
    if len(summary.results) != len(summary.selected_company_ids):
        raise ValueError("A completed Ratsit scan must have one result per company")

    result_rows = [
        (
            result.scan_id,
            result.company_id,
            result.outcome,
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
    scan_status = "success" if summary.failure_count == 0 else "completed_with_failures"
    scan_row = (
        summary.scan_id,
        scan_status,
        list(summary.selected_company_ids),
        len(summary.selected_company_ids),
        len(summary.results),
        summary.success_count,
        summary.failure_count,
        summary.reused_report_count,
        summary.written_object_count,
        RATSIT_S3_BUCKET,
        f"{RATSIT_S3_PREFIX}/",
        RATSIT_SCHEMA_VERSION,
        RATSIT_PARSER_VERSION,
        summary.started_at,
        summary.completed_at,
        "",
        "",
        indexed_at,
    )
    with clickhouse.get_connection() as client:
        client.execute(
            f"""
            INSERT INTO {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE}
            ({", ".join(RATSIT_RESULT_COLUMNS)}) VALUES
            """,
            result_rows,
        )
        client.execute(
            f"""
            INSERT INTO {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_SCAN_TABLE}
            ({", ".join(RATSIT_SCAN_COLUMNS)}) VALUES
            """,
            [scan_row],
        )
    return len(result_rows)


def record_failed_ratsit_scan(
    clickhouse: ClickhouseResource,
    *,
    scan_id: str,
    company_ids: tuple[str, ...],
    started_at: datetime,
    error_type: str,
    completed_at: datetime | None = None,
) -> None:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_SCAN_TABLE,),
    )
    failed_at = completed_at or datetime.now(UTC)
    _require_aware_timestamp(failed_at, label="failed scan completion")
    with clickhouse.get_connection() as client:
        client.execute(
            f"""
            INSERT INTO {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_SCAN_TABLE}
            ({", ".join(RATSIT_SCAN_COLUMNS)}) VALUES
            """,
            [
                (
                    scan_id,
                    "failed",
                    list(company_ids),
                    len(company_ids),
                    0,
                    0,
                    0,
                    0,
                    0,
                    RATSIT_S3_BUCKET,
                    f"{RATSIT_S3_PREFIX}/",
                    RATSIT_SCHEMA_VERSION,
                    RATSIT_PARSER_VERSION,
                    started_at,
                    failed_at,
                    error_type[:128],
                    "Ratsit scan failed before all company results were persisted",
                    failed_at,
                )
            ],
        )


def _company_prefix(company_id: str) -> str:
    if re.fullmatch(r"[0-9]{10}", company_id) is None:
        raise ValueError("Ratsit company ID must contain exactly ten digits")
    return f"{RATSIT_S3_PREFIX}/company_id={company_id}"


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
    group_name="sweden_ratsit",
    kinds={"python", "browser", "html", "json", "s3", "clickhouse", "ratsit"},
    tags={
        "country": "sweden",
        "source": "ratsit",
        "source_name": "sweden_ratsit",
        "entity_type": "company",
        "layer": "scan_dispatch",
    },
    pool=RATSIT_BROWSER_POOL,
    description=(
        "Renders and parses the same 20 Ratsit company pages in one headless "
        "CloakBrowser, with request starts at least two seconds apart. Every "
        "company outcome is indexed by Dagster run ID in ClickHouse. Changed "
        "reports are written to per-company run-ID keys; identical report "
        "hashes reuse the prior S3 object."
    ),
)
def se_ratsit_scan_dispatch(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_ratsit_browser: SwedenRatsitBrowserResource,
    sweden_ratsit_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    scan_id = context.run.run_id
    started_at = datetime.now(UTC)
    context.log.info(
        "Starting Ratsit scan: scan_id=%s companies=%s request_interval_seconds=%s",
        scan_id,
        len(RATSIT_COMPANY_IDS),
        sweden_ratsit_browser.request_interval_seconds,
    )
    try:
        reusable_reports = load_reusable_ratsit_reports(
            clickhouse,
            RATSIT_COMPANY_IDS,
        )
        summary = write_ratsit_scan(
            object_store=sweden_ratsit_object_store,
            ratsit=sweden_ratsit_browser,
            company_ids=RATSIT_COMPANY_IDS,
            scan_id=scan_id,
            reusable_reports=reusable_reports,
            started_at=started_at,
        )
        indexed_result_count = persist_ratsit_scan(clickhouse, summary)
    except Exception as error:
        try:
            record_failed_ratsit_scan(
                clickhouse,
                scan_id=scan_id,
                company_ids=RATSIT_COMPANY_IDS,
                started_at=started_at,
                error_type=type(error).__name__,
            )
        except Exception:
            context.log.exception("Could not record the failed Ratsit scan")
        raise

    context.log.info(
        "Finished Ratsit scan: scan_id=%s successes=%s failures=%s reused=%s "
        "objects_written=%s",
        scan_id,
        summary.success_count,
        summary.failure_count,
        summary.reused_report_count,
        summary.written_object_count,
    )
    return dg.MaterializeResult(
        metadata={
            "scan_id": scan_id,
            "selected_company_count": len(summary.selected_company_ids),
            "success_count": summary.success_count,
            "failure_count": summary.failure_count,
            "reused_report_count": summary.reused_report_count,
            "diagnostic_html_count": summary.diagnostic_html_count,
            "written_object_count": summary.written_object_count,
            "indexed_result_count": indexed_result_count,
            "company_ids": list(summary.selected_company_ids),
            "result_object_keys": list(summary.result_object_keys),
            "result_table": (f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE}"),
            "scan_table": f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_SCAN_TABLE}",
            "headless": sweden_ratsit_browser.headless,
            "request_interval_seconds": (
                sweden_ratsit_browser.request_interval_seconds
            ),
            "schema_version": RATSIT_SCHEMA_VERSION,
            "parser_version": RATSIT_PARSER_VERSION,
            "s3_bucket": RATSIT_S3_BUCKET,
            "s3_prefix": f"{RATSIT_S3_PREFIX}/",
        }
    )


se_ratsit_scan_dispatch_job = dg.define_asset_job(
    name="se_ratsit_scan_dispatch_job",
    selection=dg.AssetSelection.assets(se_ratsit_scan_dispatch),
    description="Run the fixed 20-company Ratsit scan.",
)


defs = dg.Definitions(
    assets=[se_ratsit_scan_dispatch],
    jobs=[se_ratsit_scan_dispatch_job],
    resources={
        "sweden_ratsit_browser": SwedenRatsitBrowserResource(),
        "sweden_ratsit_object_store": ObjectStoreResource(bucket=RATSIT_S3_BUCKET),
    },
)
