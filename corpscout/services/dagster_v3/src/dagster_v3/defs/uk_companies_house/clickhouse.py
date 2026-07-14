from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.uk_companies_house import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_uk_companies_house_clickhouse_companies(
    *,
    duckdb_connection: Any,
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
            "Exporting UK Companies House companies to ClickHouse: table=%s",
            tables.QUALIFIED_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
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
    duckdb_connection: Any,
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
            "Exporting UK Companies House industries to ClickHouse: table=%s",
            tables.QUALIFIED_INDUSTRIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
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
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    truncate: bool = True,
    log: Callable[..., object] | None = None,
) -> int:
    """Export the DuckDB metrics table to corpscout.gb_financial_metrics.

    truncate=True replaces the table (archive full-refresh); truncate=False appends
    (the on-demand API fetcher) — ReplacingMergeTree(resolved_at) dedups by company.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.UK_DATABASE,
        tables=(tables.FINANCIAL_METRICS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting UK Companies House financial metrics to ClickHouse: table=%s",
            tables.QUALIFIED_FINANCIAL_METRICS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.FINANCIAL_METRICS_TABLE,
            clickhouse_database=tables.UK_DATABASE,
            clickhouse_table=tables.FINANCIAL_METRICS_TABLE_CH,
            columns=tables.GB_FINANCIAL_METRICS_EXPORT_COLUMNS,
            truncate=truncate,
        )
    if log is not None:
        log("Finished UK Companies House financial metrics ClickHouse export: rows=%s", rows)
    return rows
