from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE


def export_latvia_ur_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.lv_companies with the DuckDB register entities table.

    The ClickHouse schema is owned by the migration; this only asserts the table
    exists and atomically swaps in the freshly loaded rows.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.LATVIA_UR_DATABASE,
        tables=(tables.LV_COMPANIES_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Latvia UR companies to ClickHouse: table=%s",
            tables.QUALIFIED_LV_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=ENTITIES_TABLE,
            clickhouse_database=tables.LATVIA_UR_DATABASE,
            clickhouse_table=tables.LV_COMPANIES_TABLE,
            columns=tables.LV_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Latvia UR companies ClickHouse export: rows=%s", rows)
    return rows


def export_latvia_ur_clickhouse_financial_statements(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.lv_financial_statements with the wide DuckDB pivot table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.LATVIA_UR_DATABASE,
        tables=(tables.LV_FINANCIAL_STATEMENTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Latvia UR financial statements to ClickHouse: table=%s",
            tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.FINANCIAL_STATEMENTS_WIDE_TABLE,
            clickhouse_database=tables.LATVIA_UR_DATABASE,
            clickhouse_table=tables.LV_FINANCIAL_STATEMENTS_TABLE,
            columns=tables.LV_FINANCIAL_STATEMENTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Latvia UR financial statements ClickHouse export: rows=%s", rows)
    return rows


def export_latvia_ur_clickhouse_financial_metrics(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.lv_financial_metrics with the DuckDB metrics table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.LATVIA_UR_DATABASE,
        tables=(tables.LV_FINANCIAL_METRICS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Latvia UR financial metrics to ClickHouse: table=%s",
            tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.FINANCIAL_METRICS_WIDE_TABLE,
            clickhouse_database=tables.LATVIA_UR_DATABASE,
            clickhouse_table=tables.LV_FINANCIAL_METRICS_TABLE,
            columns=tables.LV_FINANCIAL_METRICS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Latvia UR financial metrics ClickHouse export: rows=%s", rows)
    return rows
