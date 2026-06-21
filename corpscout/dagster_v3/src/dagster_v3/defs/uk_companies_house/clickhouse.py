from collections.abc import Callable
from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.uk_companies_house import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_uk_companies_house_clickhouse_companies(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.gb_companies with the DuckDB companies table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.UK_DATABASE,
        tables=(tables.COMPANIES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting UK Companies House companies to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANIES_TABLE,
            clickhouse_database=tables.UK_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE_CH,
            columns=tables.GB_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished UK Companies House companies ClickHouse export: rows=%s", rows)
    return rows


def export_uk_companies_house_clickhouse_industries(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.gb_industries with the DuckDB industries table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.UK_DATABASE,
        tables=(tables.INDUSTRIES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting UK Companies House industries to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_INDUSTRIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.INDUSTRIES_RAW_TABLE,
            clickhouse_database=tables.UK_DATABASE,
            clickhouse_table=tables.INDUSTRIES_TABLE_CH,
            columns=tables.GB_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished UK Companies House industries ClickHouse export: rows=%s", rows)
    return rows


def export_uk_companies_house_clickhouse_financial_metrics(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.gb_financial_metrics with the DuckDB metrics table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.UK_DATABASE,
        tables=(tables.FINANCIAL_METRICS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting UK Companies House financial metrics to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_FINANCIAL_METRICS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.FINANCIAL_METRICS_TABLE,
            clickhouse_database=tables.UK_DATABASE,
            clickhouse_table=tables.FINANCIAL_METRICS_TABLE_CH,
            columns=tables.GB_FINANCIAL_METRICS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished UK Companies House financial metrics ClickHouse export: rows=%s", rows)
    return rows
