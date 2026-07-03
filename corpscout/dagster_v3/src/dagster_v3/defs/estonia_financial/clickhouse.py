from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.estonia_ar import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_estonia_financial_statements_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.ee_financial_statements with the wide DuckDB statements table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESTONIA_AR_DATABASE,
        tables=(tables.EE_FINANCIAL_STATEMENTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Estonia financial statements to ClickHouse: table=%s",
            tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.FINANCIAL_STATEMENTS_WIDE_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_FINANCIAL_STATEMENTS_TABLE,
            columns=tables.EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Estonia financial statements ClickHouse export: rows=%s", rows)
    return rows


def export_estonia_financial_metrics_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.ee_financial_metrics with the DuckDB metrics table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESTONIA_AR_DATABASE,
        tables=(tables.EE_FINANCIAL_METRICS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Estonia financial metrics to ClickHouse: table=%s",
            tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.FINANCIAL_METRICS_WIDE_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_FINANCIAL_METRICS_TABLE,
            columns=tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Estonia financial metrics ClickHouse export: rows=%s", rows)
    return rows

