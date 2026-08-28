import gzip
import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
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
    "5560001629",  # Nouryon International AB
    "5560001991",  # Bengtsfors Kraft- och industri Aktiebolag
    "5560002221",  # Nordic Paper Seffle AB
    "5560003575",  # Axfood Snabbgross AB
    "5560005190",  # Fastighetsaktiebolaget Mösseberg
    "5560005323",  # Göteborgs is Aktiebolag
    "5560005331",  # Husqvarna AB
    "5560006628",  # KFUK-KFUM:s i Eskilstuna Fastighetsaktiebolag
    "5560008038",  # Fastighetsaktiebolaget Skeppsbron
    "5560011248",  # Karlskoga Industrifastighets AB
    "5560013301",  # Holmen Aktiebolag
    "5560016122",  # Gränges AB
    "5560016817",  # Fastighetsaktiebolaget Bubblan i Åre
    "5560019282",  # Hellekis Säteri Aktiebolag
    "5560019399",  # Växthuset i Ås Aktiebolag
    "5560019480",  # Sundsvalls Hantverksförening Aktiebolag
    "5560020017",  # Malmö Frimurare Byggnads Aktiebolag
    "5560020165",  # Fastighets AB Bergshamra sågen
    "5560020231",  # Nobel Biocare AB
    "5560022617",  # Ursviken Technology AB
    "5560022807",  # Aktiebolaget Carl Söderberg
    "5560025156",  # Svenska Granitindustri Aktiebolag
    "5560026113",  # Gränges Finspång AB
    "5560026311",  # Byggnadsaktiebolaget Engelbrekt
    "5560026501",  # Virgula Aktiebolag
    "5560026766",  # Biovestor Aktiebolag
    "5560027327",  # Gårda Fabrikers Aktiebolag
    "5560028150",  # Härlingstorps Aktiebolag
    "5560029265",  # Odd Fellow i Göteborg Förvaltnings AB
    "5560029539",  # Medevi Brunn AB
    "5560029729",  # Aktiebolaget Jersey Depot
    "5560030354",  # Electrolux Professional AB (publ)
    "5560031212",  # Stockholms Borstbinderi Aktiebolag
    "5560031386",  # Tidningshuset Storstadspress Aktiebolag
    "5560031410",  # Ammers Såg & Qvarn AB
    "5560031634",  # Alen Livs AB
    "5560032046",  # Golltan AB
    "5560032764",  # Rederiaktiebolaget Roslagen
    "5560032921",  # Siemens Aktiebolag
    "5560033143",  # ActiVera Sweden AB
    "5560033457",  # Gertrud Fastigheter AB
    "5560033978",  # Aktiebolaget Borås Tidning
    "5560035643",  # Karlsö jagt- och djurskyddsförenings aktiebolag
    "5560035874",  # Talent Plastics Laxå AB
    "5560041161",  # Nordemans Förvaltnings Aktiebolag
    "5560041708",  # Aktiebolaget 3127 Alfhem
    "5560042060",  # Mo och Domsjö Aktiebolag
    "5560044736",  # Sten och Tegelaktiebolaget
    "5560047127",  # Lyckornagruppen AB
    "5560048372",  # Svenska Fastighetsaktiebolaget
    "5560049529",  # Masmästaren Näktergalen AB
    "5560050204",  # Carl Folke & Co Aktiebolag
    "5560050832",  # C.J.Walls sågeri och trävaru aktiebolag
    "5560052358",  # Festspecialisten Buttericks Aktiebolag
    "5560053331",  # SILIKATENS SERVICE Aktiebolag
    "5560057662",  # Urbana Holding AB
    "5560059759",  # Aktiebolaget Gäddeglo Tegelbruk
    "5560059775",  # Byggnadsaktiebolaget Unitas i Wisby
    "5560062761",  # Gripsholms-Mariefreds Ångfartygsaktiebolag
    "5560063421",  # Trelleborg AB
    "5560068321",  # Sätuna aktiebolag
    "5560068586",  # BonBalance AB
    "5560068701",  # Aktiebolaget Himmelsö
    "5560068990",  # Fastighetsaktiebolaget Vinaman
    "5560069840",  # Starbo Bruk Aktiebolag
    "5560070756",  # Tantum AB
    "5560071473",  # Smörjteknik Norden AB
    "5560071671",  # Starfors Säteri AB
    "5560073800",  # Västkustens Skogs AB
    "5560073842",  # Fjällnäs Aktiebolag
    "5560074626",  # Söderströmgruppen Aktiebolag
    "5560075557",  # Handelsaktiebolaget i Ousby
    "5560079799",  # Triangelbolaget D4 Aktiebolag
    "5560081621",  # Elanders AB
    "5560082892",  # Flerohopps Bruks Aktiebolag
    "5560083585",  # Ratos AB
    "5560085440",  # Disperator AB
    "5560086661",  # ABW Equipment AB
    "5560087743",  # Aktiebolaget Karlshälls Granitindustri
    "5560088402",  # Aktiebolaget Ingarö Strand
)
RATSIT_MAX_COMPANIES = RATSIT_HARD_MAX_COMPANIES
RATSIT_S3_BUCKET = "source-sweden-ratsit"
RATSIT_S3_PREFIX = "sweden_ratsit/pilot"
RATSIT_SCHEMA_VERSION = 1
RATSIT_PARSER_VERSION = "ratsit-html-v1"
RATSIT_BROWSER_POOL = "sweden_ratsit_browser"
RATSIT_NORMALIZE_POOL = "sweden_ratsit_normalize"
RATSIT_CLICKHOUSE_DATABASE = "corpscout"
RATSIT_RESULT_TABLE = "se_company_ratsit"

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
    not_found_count = 0
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
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_RESULT_TABLE,),
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
        "Renders and parses the same 100 Ratsit company pages with four parallel "
        "headless CloakBrowsers: one direct and three proxied. Companies are "
        "assigned round-robin, and each browser spaces request starts by at "
        "least two seconds. Every company outcome is indexed by Dagster run ID "
        "in ClickHouse. Changed reports are written to per-company run-ID keys; "
        "identical report hashes reuse the prior S3 object."
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
        "Starting Ratsit scan: scan_id=%s companies=%s browser_workers=%s "
        "request_interval_seconds=%s",
        scan_id,
        len(RATSIT_COMPANY_IDS),
        RATSIT_BROWSER_WORKER_COUNT,
        sweden_ratsit_browser.request_interval_seconds,
    )
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
    http_429_counts_by_route = {
        worker_name: sum(
            1
            for result in summary.results
            if result.http_status == 429
            and (result.proxy_name if result.proxy_name else "direct") == worker_name
        )
        for worker_name, _ in ratsit_round_robin_assignments(
            summary.selected_company_ids
        )
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
    return dg.MaterializeResult(
        metadata={
            "scan_id": scan_id,
            "selected_company_count": len(summary.selected_company_ids),
            "success_count": summary.success_count,
            "not_found_count": summary.not_found_count,
            "failure_count": summary.failure_count,
            "reused_report_count": summary.reused_report_count,
            "diagnostic_html_count": summary.diagnostic_html_count,
            "written_object_count": summary.written_object_count,
            "indexed_result_count": indexed_result_count,
            "company_ids": list(summary.selected_company_ids),
            "browser_assignments": {
                worker_name: list(company_ids)
                for worker_name, company_ids in ratsit_round_robin_assignments(
                    summary.selected_company_ids
                )
            },
            "browser_worker_count": RATSIT_BROWSER_WORKER_COUNT,
            "proxy_browser_count": RATSIT_BROWSER_WORKER_COUNT - 1,
            "http_429_count": http_429_count,
            "http_429_counts_by_route": http_429_counts_by_route,
            "result_object_keys": list(summary.result_object_keys),
            "result_table": (f"{RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_RESULT_TABLE}"),
            "headless": sweden_ratsit_browser.headless,
            "request_interval_seconds": (
                sweden_ratsit_browser.request_interval_seconds
            ),
            "effective_average_request_interval_seconds": (
                sweden_ratsit_browser.request_interval_seconds
                / RATSIT_BROWSER_WORKER_COUNT
            ),
            "schema_version": RATSIT_SCHEMA_VERSION,
            "parser_version": RATSIT_PARSER_VERSION,
            "s3_bucket": RATSIT_S3_BUCKET,
            "s3_prefix": f"{RATSIT_S3_PREFIX}/",
        }
    )


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
    pool=RATSIT_NORMALIZE_POOL,
    can_subset=False,
)
def se_ratsit_normalized(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_ratsit_object_store: ObjectStoreResource,
) -> Iterator[dg.MaterializeResult]:
    """Normalize the latest successful report per company without launching browsers."""
    normalized_at = datetime.now(UTC)
    selection = select_latest_unnormalized_ratsit_reports(clickhouse)
    nace_class_codes = load_nace_rev_2_1_class_codes(clickhouse)
    context.log.info(
        "Starting Ratsit JSON normalization: latest_successes=%s "
        "already_normalized=%s candidates=%s normalizer_version=%s",
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
        "latest_success_count": selection.latest_success_count,
        "already_normalized_count": selection.already_normalized_count,
        "normalized_report_count": len(normalized_reports),
        "normalizer_version": RATSIT_NORMALIZER_VERSION,
        "nace_revision": RATSIT_NACE_REVISION,
        "nace_class_reference_count": len(nace_class_codes),
        "normalized_at": normalized_at.isoformat(),
        "result_object_keys": [
            report.result_object_key for report in selection.reports
        ],
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
    description="Run the fixed 100-company Ratsit scan.",
)

se_ratsit_normalize_job = dg.define_asset_job(
    name="se_ratsit_normalize_job",
    selection=dg.AssetSelection.assets(*RATSIT_NORMALIZED_TABLES),
    description=(
        "Normalize each company's latest successful Ratsit S3 report into "
        "source-specific ClickHouse tables."
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
