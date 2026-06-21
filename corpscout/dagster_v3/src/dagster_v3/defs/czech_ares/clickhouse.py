from collections.abc import Callable
from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.czech_ares import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_czech_ares_clickhouse_companies(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.cz_companies with the DuckDB companies table."""
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.CZECH_DATABASE, tables=(tables.COMPANIES_TABLE_CH,)
    )
    if log is not None:
        log("Exporting Czech ARES companies: table=%s", tables.QUALIFIED_COMPANIES_TABLE)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANIES_TABLE,
            clickhouse_database=tables.CZECH_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE_CH,
            columns=tables.CZ_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Czech ARES companies ClickHouse export: rows=%s", rows)
    return rows


def export_czech_ares_clickhouse_industries(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.cz_industries with the DuckDB industries table."""
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.CZECH_DATABASE, tables=(tables.INDUSTRIES_TABLE_CH,)
    )
    if log is not None:
        log("Exporting Czech ARES industries: table=%s", tables.QUALIFIED_INDUSTRIES_TABLE)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.INDUSTRIES_RAW_TABLE,
            clickhouse_database=tables.CZECH_DATABASE,
            clickhouse_table=tables.INDUSTRIES_TABLE_CH,
            columns=tables.CZ_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Czech ARES industries ClickHouse export: rows=%s", rows)
    return rows
