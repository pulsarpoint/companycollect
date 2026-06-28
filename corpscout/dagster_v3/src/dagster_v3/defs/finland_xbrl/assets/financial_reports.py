import json
import time
from collections.abc import Iterator
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

import dagster as dg
import dlt
from dagster_duckdb import DuckDBResource
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers.requests import Client as DltRequestsClient
from pydantic import field_validator

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    read_only_duckdb_connection,
)
from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    FINLAND_XBRL_DUCKDB_POOL,
    PRH_XBRL_REGISTRATION_SEARCH_START,
    XBRL_BASE_URL,
    XBRL_DLT_DATASET_NAME,
    XBRL_DLT_FINANCIAL_REPORTS_TABLE,
    XBRL_TIMEOUT_SECONDS,
    _duckdb_table_exists,
    _registration_window,
)
from dagster_v3.defs.finland_xbrl.resources import HttpSession

class XbrlFinancialReportsConfig(dg.Config):
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS

    @field_validator(
        "request_delay_seconds",
        "retry_initial_delay_seconds",
        "retry_max_delay_seconds",
    )
    @classmethod
    def validate_non_negative_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("delay values must be zero or greater")
        return value

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_retries must be greater than zero")
        return value


@dlt.source(name="finland_xbrl")
def finland_xbrl_financial_reports_source(
    *,
    registered_date_start: str,
    registered_date_end: str,
    base_url: str = XBRL_BASE_URL,
    timeout_seconds: int = XBRL_TIMEOUT_SECONDS,
    user_agent: str = "corpscout-dagster-v3-dev/0.1",
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    run_id: str = "",
    session: HttpSession | None = None,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[[str], None] | None = None,
) -> DltResource:
    return _financial_reports_resource(
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        request_delay_seconds=request_delay_seconds,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        run_id=run_id,
        session=session,
        sleep=sleep,
        log_info=log_info,
    )


@dlt.resource(
    name=XBRL_DLT_FINANCIAL_REPORTS_TABLE,
    write_disposition="merge",
    primary_key=("business_id", "financial_date", "registration_date"),
)
def _financial_reports_resource(
    *,
    registered_date_start: str,
    registered_date_end: str,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    request_delay_seconds: float,
    max_retries: int,
    retry_initial_delay_seconds: float,
    retry_max_delay_seconds: float,
    run_id: str,
    session: HttpSession | None,
    sleep: Callable[[float], None],
    log_info: Callable[[str], None] | None,
) -> Iterator[dict[str, Any]]:
    _ensure_supported_registered_date_start(registered_date_start)
    page_number = 1
    source_record_number = 1
    report_count = 0
    non_empty_page_count = 0
    http_client = session or _financial_reports_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
    )
    _log_financial_reports_discovery(
        log_info,
        f"PRH XBRL financial reports discovery {registered_date_start}..{registered_date_end} started",
    )
    while True:
        payload = _download_financial_reports_page(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            page_number=page_number,
            session=http_client,
        )
        financials = _financials_from_payload(payload)
        if not financials:
            _log_financial_reports_discovery(
                log_info,
                "PRH XBRL financial reports discovery "
                f"{registered_date_start}..{registered_date_end} page {page_number} "
                "returned 0 reports; stopping",
            )
            break
        non_empty_page_count += 1
        report_count += len(financials)
        _log_financial_reports_discovery(
            log_info,
            "PRH XBRL financial reports discovery "
            f"{registered_date_start}..{registered_date_end} page {page_number} "
            f"returned {len(financials)} reports",
        )
        for page_record_number, financial in enumerate(financials, start=1):
            yield _dlt_financial_report_row(
                financial,
                registered_date_start=registered_date_start,
                registered_date_end=registered_date_end,
                source_page_number=page_number,
                source_page_record_number=page_record_number,
                source_record_number=source_record_number,
                run_id=run_id,
            )
            source_record_number += 1
        page_number += 1
        if request_delay_seconds > 0:
            sleep(request_delay_seconds)
    _log_financial_reports_discovery(
        log_info,
        "PRH XBRL financial reports discovery "
        f"{registered_date_start}..{registered_date_end} completed: {report_count} "
        f"reports across {non_empty_page_count} non-empty pages",
    )


def _log_financial_reports_discovery(
    log_info: Callable[[str], None] | None,
    message: str,
) -> None:
    if log_info is not None:
        log_info(message)


def run_finland_xbrl_financial_reports_dlt_pipeline(
    *,
    database_path: str | Path,
    registered_date_start: str,
    registered_date_end: str,
    run_id: str,
    session: HttpSession | None = None,
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    log_info: Callable[[str], None] | None = None,
) -> Any:
    return finland_xbrl_financial_reports_pipeline(database_path).run(
        finland_xbrl_financial_reports_source(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            run_id=run_id,
            session=session,
            request_delay_seconds=request_delay_seconds,
            max_retries=max_retries,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
            log_info=log_info,
        )
    )


def finland_xbrl_financial_reports_pipeline(database_path: str | Path) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="finland_xbrl_financial_reports",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=XBRL_DLT_DATASET_NAME,
        dev_mode=False,
    )


def _materialize_financial_reports_window(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    source_duckdb: DuckDBResource,
    *,
    registered_date_start: str,
    registered_date_end: str,
) -> dg.MaterializeResult:
    context.log.info(
        "XBRL financial reports partition %s: loading reports registered %s..%s",
        context.partition_key,
        registered_date_start,
        registered_date_end,
    )
    run_finland_xbrl_financial_reports_dlt_pipeline(
        database_path=duckdb_database_path(source_duckdb),
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
        run_id=context.run_id,
        request_delay_seconds=config.request_delay_seconds,
        max_retries=config.max_retries,
        retry_initial_delay_seconds=config.retry_initial_delay_seconds,
        retry_max_delay_seconds=config.retry_max_delay_seconds,
        log_info=context.log.info,
    )
    row_count = financial_reports_duckdb_row_count(source_duckdb)
    context.log.info(
        "XBRL financial reports partition %s complete: registered %s..%s table_row_count=%d",
        context.partition_key,
        registered_date_start,
        registered_date_end,
        row_count,
    )
    return dg.MaterializeResult(
        metadata={
            "partition": context.partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "row_count": row_count,
        }
    )


@dg.asset(
    name="finland_xbrl_financial_reports_backfill_duckdb",
    group_name="finland_xbrl",
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "dlt", "duckdb"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Monthly backfill writer for PRH XBRL financial report listings by registration date.",
)
def finland_xbrl_financial_reports_backfill_duckdb(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return _materialize_financial_reports_window(
        context,
        config,
        source_duckdb,
        registered_date_start=start,
        registered_date_end=end,
    )


@dg.asset(
    name="finland_xbrl_financial_reports_incremental_duckdb",
    group_name="finland_xbrl",
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "dlt", "duckdb"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Daily incremental writer for PRH XBRL financial report listings by registration date.",
)
def finland_xbrl_financial_reports_incremental_duckdb(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return _materialize_financial_reports_window(
        context,
        config,
        source_duckdb,
        registered_date_start=start,
        registered_date_end=end,
    )


@dg.asset(
    name="finland_xbrl_financial_reports_duckdb",
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_financial_reports_backfill_duckdb,
        finland_xbrl_financial_reports_incremental_duckdb,
    ],
    kinds={"duckdb"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
    description="Catalog marker for the shared PRH XBRL financial report listing DuckDB table.",
)
def finland_xbrl_financial_reports_duckdb(
    context: dg.AssetExecutionContext,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info("Loading Finland XBRL financial reports DuckDB table marker")
    row_count = financial_reports_duckdb_row_count(source_duckdb)
    context.log.info("Finland XBRL financial reports DuckDB table row_count=%d", row_count)
    return dg.MaterializeResult(
        metadata={
            "duckdb_schema": XBRL_DLT_DATASET_NAME,
            "duckdb_table": XBRL_DLT_FINANCIAL_REPORTS_TABLE,
            "row_count": row_count,
        }
    )

def _download_financial_reports_page(
    *,
    registered_date_start: str,
    registered_date_end: str,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    page_number: int,
    session: HttpSession | None,
) -> dict[str, Any]:
    http_session = session or _financial_reports_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
        retry_initial_delay_seconds=DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
        retry_max_delay_seconds=DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    )
    headers = getattr(http_session, "headers", None)
    if isinstance(headers, dict):
        headers["User-Agent"] = user_agent
    response = http_session.get(
        f"{base_url}/all_financial_statements",
        params={
            "registeredDateStart": registered_date_start,
            "registeredDateEnd": registered_date_end,
            "page": page_number,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _ensure_supported_registered_date_start(registered_date_start: str) -> None:
    if date.fromisoformat(registered_date_start) >= date.fromisoformat(
        PRH_XBRL_REGISTRATION_SEARCH_START
    ):
        return
    raise ValueError(
        "PRH XBRL API only supports registration date searches starting on or after "
        f"{PRH_XBRL_REGISTRATION_SEARCH_START}; got {registered_date_start}"
    )


def _financial_reports_http_client(
    *,
    timeout_seconds: int,
    user_agent: str,
    max_retries: int,
    retry_initial_delay_seconds: float,
    retry_max_delay_seconds: float,
) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        request_max_attempts=max_retries,
        request_backoff_factor=retry_initial_delay_seconds,
        request_max_retry_delay=retry_max_delay_seconds,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": user_agent}},
    )


def _financials_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    financials = payload.get("financials") or []
    return [financial for financial in financials if isinstance(financial, dict)]


def _dlt_financial_report_row(
    financial: dict[str, Any],
    *,
    registered_date_start: str,
    registered_date_end: str,
    source_page_number: int,
    source_page_record_number: int,
    source_record_number: int,
    run_id: str,
) -> dict[str, Any]:
    business_id = str(financial.get("businessId") or "").strip()
    financial_date = str(financial.get("financialDate") or "").strip()
    registration_date = str(financial.get("registrationDate") or "").strip()
    return {
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date,
        "discovery_registered_date_start": registered_date_start,
        "discovery_registered_date_end": registered_date_end,
        "source_run_id": run_id,
        "source_page_number": source_page_number,
        "source_page_record_number": source_page_record_number,
        "source_record_number": source_record_number,
        "source_payload_hash": _source_payload_hash(financial),
        "raw_financial": json.dumps(financial, ensure_ascii=False, separators=(",", ":")),
    }


def _source_payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(body.encode("utf-8")).hexdigest()


def financial_reports_duckdb_row_count(source_duckdb: DuckDBResource) -> int:
    if not duckdb_database_path(source_duckdb).exists():
        return 0
    with read_only_duckdb_connection(source_duckdb) as connection:
        if not _duckdb_table_exists(
            connection, table=XBRL_DLT_FINANCIAL_REPORTS_TABLE
        ):
            return 0
        return int(
            connection.execute(
                f"select count(*) from "
                f"{XBRL_DLT_DATASET_NAME}.{XBRL_DLT_FINANCIAL_REPORTS_TABLE}"
            ).fetchone()[0]
        )
