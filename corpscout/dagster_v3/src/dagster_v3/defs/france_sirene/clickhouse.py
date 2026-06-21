from collections.abc import Callable
from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.france_sirene import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_france_sirene_clickhouse_companies(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.fr_companies with the DuckDB companies table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.FRANCE_SIRENE_DATABASE,
        tables=(tables.COMPANIES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting France SIRENE companies to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANIES_TABLE,
            clickhouse_database=tables.FRANCE_SIRENE_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE_CH,
            columns=tables.FR_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished France SIRENE companies ClickHouse export: rows=%s", rows)
    return rows


def export_france_sirene_clickhouse_industries(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.fr_industries with the DuckDB industries table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.FRANCE_SIRENE_DATABASE,
        tables=(tables.INDUSTRIES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting France SIRENE industries to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_INDUSTRIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.INDUSTRIES_RAW_TABLE,
            clickhouse_database=tables.FRANCE_SIRENE_DATABASE,
            clickhouse_table=tables.INDUSTRIES_TABLE_CH,
            columns=tables.FR_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished France SIRENE industries ClickHouse export: rows=%s", rows)
    return rows
