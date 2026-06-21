from collections.abc import Callable
from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.finland_financials import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_finland_financials_clickhouse_metrics(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    truncate: bool = False,
    log: Callable[..., object] | None = None,
) -> int:
    """Export the DuckDB metrics table to corpscout.fi_financial_metrics.

    Appends by default (ReplacingMergeTree dedups by business_id/financial_date/
    statement_key) so bounded runs accumulate.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.FINLAND_DATABASE,
        tables=(tables.METRICS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Finland financial metrics to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_METRICS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.METRICS_TABLE,
            clickhouse_database=tables.FINLAND_DATABASE,
            clickhouse_table=tables.METRICS_TABLE_CH,
            columns=tables.FI_FINANCIAL_METRICS_COLUMNS,
            truncate=truncate,
        )
    if log is not None:
        log("Finished Finland financial metrics ClickHouse export: rows=%s", rows)
    return rows
