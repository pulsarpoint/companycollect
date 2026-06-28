import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl

from dagster_v3.defs.finland_xbrl import tables

XBRL_BUCKET = "source-finland-prh-xbrl"
RAW_XML_DOCUMENTS_OBJECT_KEY = f"raw/{tables.XML_DOCUMENTS_TABLE}.parquet"
XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
XBRL_TIMEOUT_SECONDS = 120
XBRL_DLT_DATASET_NAME = "finland_prh_xbrl"
XBRL_DLT_FINANCIAL_REPORTS_TABLE = "financial_reports"
XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE = "eligible_financial_reports"
XBRL_ELIGIBLE_COMPANIES_TABLE = "eligible_companies"
DEFAULT_XBRL_REQUEST_DELAY_SECONDS = 1.0
DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS = 6
DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS = 30.0
DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS = 480.0
PRH_XBRL_REGISTRATION_SEARCH_START = "2023-07-01"
FINLAND_XBRL_DUCKDB_POOL = "finland_xbrl_duckdb"
BACKFILL_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2025-06-01", end_date="2026-06-01"
)
DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
    hour_offset=6,
    timezone="Europe/Belgrade",
)

FINLAND_XBRL_DBT_PROJECT_DIR = Path(__file__).parents[1] / "dbt"
_XBRL_DUCKDB_PATH = Path("data/finland_xbrl.duckdb").expanduser()
if not _XBRL_DUCKDB_PATH.is_absolute():
    _XBRL_DUCKDB_PATH = _XBRL_DUCKDB_PATH.resolve()
os.environ["FINLAND_XBRL_DUCKDB_PATH"] = str(_XBRL_DUCKDB_PATH)


def _registration_window(context: dg.AssetExecutionContext) -> tuple[str, str]:
    window = context.partition_time_window
    start = window.start.date().isoformat()
    end = (window.end.date() - timedelta(days=1)).isoformat()
    return start, end

def _duckdb_table_exists(connection: Any, *, table: str) -> bool:
    return bool(
        connection.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ?
              and table_name = ?
            """,
            [XBRL_DLT_DATASET_NAME, table],
        ).fetchone()[0]
    )


def _duckdb_column_type(column: str) -> str:
    dtype = _xbrl_column_dtype(column)
    if dtype == pl.Boolean:
        return "boolean"
    if dtype == pl.Int64:
        return "bigint"
    if dtype == pl.Float64:
        return "double"
    return "varchar"

def _optional_iso_date(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("date config values must be strings")
    stripped = value.strip()
    if not stripped:
        return None
    _parse_iso_date(stripped, field_name="date config value")
    return stripped


def _parse_iso_date(value: str, *, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from error

def _xbrl_table_frame(table: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={column: _xbrl_column_dtype(column) for column in tables.TABLE_COLUMNS[table]},
    )


def _table_row(table: str, row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.TABLE_COLUMNS[table]}


def _xbrl_column_dtype(column: str) -> pl.DataType:
    if column in {
        "xml_size_bytes",
        "statement_count",
        "business_count",
        "financial_date_count",
        "numeric_count",
        "date_count",
        "text_count",
        "current_period_count",
        "comparative_count",
        "xml_documents_count",
        "statement_documents_count",
        "facts_count",
        "statements_with_zero_facts_count",
        "duplicate_statement_keys_count",
        "parser_warning_statements_count",
        "missing_statement_business_ids_count",
        "missing_statement_financial_dates_count",
        "missing_fact_business_ids_count",
        "missing_fact_financial_dates_count",
        "facts_per_statement_min",
        "facts_per_statement_max",
        "contexts_count",
        "units_count",
        "fact_ordinal",
        "source_fact_count",
        "mapped_fact_count",
        "unmapped_numeric_fact_count",
    }:
        return pl.Int64
    if column == "facts_per_statement_avg":
        return pl.Float64
    if column in {"downloaded", "reused", "is_comparative"}:
        return pl.Boolean
    return pl.Utf8
