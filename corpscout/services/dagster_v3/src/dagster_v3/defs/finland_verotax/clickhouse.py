"""ClickHouse export for the Finland verotax source."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.finland_verotax import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_finland_verotax_records_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.fi_tax_records with the DuckDB tax_records table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.FINLAND_VEROTAX_DATABASE,
        tables=(tables.FI_TAX_RECORDS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Finland verotax records to ClickHouse: table=%s",
            tables.QUALIFIED_FI_TAX_RECORDS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.TAX_RECORDS_TABLE,
            clickhouse_database=tables.FINLAND_VEROTAX_DATABASE,
            clickhouse_table=tables.FI_TAX_RECORDS_TABLE,
            columns=tables.FI_TAX_RECORDS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Finland verotax ClickHouse export: rows=%s", rows)
    return rows
