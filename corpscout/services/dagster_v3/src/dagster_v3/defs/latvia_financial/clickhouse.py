from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_latvia_financial_statements_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.lv_financial_statements with the wide DuckDB statements table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.LATVIA_UR_DATABASE,
        tables=(tables.LV_FINANCIAL_STATEMENTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Latvia financial statements to ClickHouse: table=%s",
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
        log("Finished Latvia financial statements ClickHouse export: rows=%s", rows)
    return rows


def export_latvia_financial_metrics_clickhouse(
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
            "Exporting Latvia financial metrics to ClickHouse: table=%s",
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
        log("Finished Latvia financial metrics ClickHouse export: rows=%s", rows)
    return rows
