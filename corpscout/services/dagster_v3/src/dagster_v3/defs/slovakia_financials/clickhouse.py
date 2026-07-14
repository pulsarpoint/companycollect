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
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace corpscout.sk_financial_metrics from the DuckDB table.

    The DuckDB staging table accumulates the full decoded history (see
    metrics.build_metrics_from_batches), so the export is the repo-standard
    full-snapshot replace (stage + EXCHANGE TABLES). Refuses to blank a
    populated table when staging is empty.
    """
    row_count = int(
        duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()[0]
    )
    if row_count == 0:
        raise ValueError(
            f"refusing to replace {tables.QUALIFIED_METRICS_TABLE} with 0 rows "
            "from DuckDB staging"
        )
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.SLOVAKIA_DATABASE, tables=(tables.METRICS_TABLE_CH,)
    )
    if log is not None:
        log(
            "Exporting Slovak RÚZ metrics to ClickHouse: table=%s rows=%s",
            tables.QUALIFIED_METRICS_TABLE,
            row_count,
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
            truncate=True,
        )
    if log is not None:
        log("Finished Slovak RÚZ metrics ClickHouse export: rows=%s", rows)
    return rows
