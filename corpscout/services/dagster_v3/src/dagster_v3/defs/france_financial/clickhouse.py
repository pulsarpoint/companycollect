"""ClickHouse export for France BCE/INPI financial metrics."""

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.france_financial import tables


def export_france_financial_metrics_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace corpscout.fr_financial_metrics."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.FINANCIAL_METRICS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table=tables.METRICS_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=tables.FINANCIAL_METRICS_TABLE,
            columns=tables.FR_FINANCIAL_METRICS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Exported France financial metrics to ClickHouse: rows=%s", rows)
    return rows
