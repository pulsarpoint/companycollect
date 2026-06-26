from collections.abc import Callable

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.slovakia_financials import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_slovakia_financials_clickhouse_metrics(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
    truncate: bool = False,
    log: Callable[..., object] | None = None,
) -> int:
    """Export the DuckDB metrics table to corpscout.sk_financial_metrics.

    Appends by default — ReplacingMergeTree dedups by statement_id, so the
    bounded forward-sweep runs accumulate without duplicating statements.
    """
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.SLOVAKIA_DATABASE, tables=(tables.METRICS_TABLE_CH,)
    )
    if log is not None:
        log(
            "Exporting Slovak RÚZ metrics to ClickHouse: table=%s",
            tables.QUALIFIED_METRICS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.METRICS_TABLE,
            clickhouse_database=tables.SLOVAKIA_DATABASE,
            clickhouse_table=tables.METRICS_TABLE_CH,
            columns=tables.SK_FINANCIAL_METRICS_COLUMNS,
            truncate=truncate,
        )
    if log is not None:
        log("Finished Slovak RÚZ metrics ClickHouse export: rows=%s", rows)
    return rows
