from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_company import tables


def export_sweden_company_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace the Sweden company register ClickHouse table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(tables.COMPANIES_TABLE_CH,),
    )

    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Exporting Sweden company table to ClickHouse: table=%s",
                tables.QUALIFIED_COMPANIES_TABLE,
            )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table="companies",
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE_CH,
            columns=tables.SE_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Sweden company ClickHouse export: table=%s rows=%s", tables.QUALIFIED_COMPANIES_TABLE, rows)
    return rows


def export_sweden_company_clickhouse_addresses(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace the Sweden company addresses ClickHouse table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(tables.COMPANY_ADDRESSES_TABLE_CH,),
    )

    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Exporting Sweden company table to ClickHouse: table=%s",
                tables.QUALIFIED_COMPANY_ADDRESSES_TABLE,
            )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table="company_addresses",
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=tables.COMPANY_ADDRESSES_TABLE_CH,
            columns=tables.SE_COMPANY_ADDRESSES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log(
            "Finished Sweden company ClickHouse export: table=%s rows=%s",
            tables.QUALIFIED_COMPANY_ADDRESSES_TABLE,
            rows,
        )
    return rows


def export_sweden_company_clickhouse_industries(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace the Sweden company industries ClickHouse table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(tables.INDUSTRIES_TABLE_CH,),
    )

    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Exporting Sweden company table to ClickHouse: table=%s",
                tables.QUALIFIED_INDUSTRIES_TABLE,
            )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table="company_industry_codes",
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=tables.INDUSTRIES_TABLE_CH,
            columns=tables.SE_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log(
            "Finished Sweden company ClickHouse export: table=%s rows=%s",
            tables.QUALIFIED_INDUSTRIES_TABLE,
            rows,
        )
    return rows
