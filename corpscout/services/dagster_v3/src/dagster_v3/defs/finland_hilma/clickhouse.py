"""ClickHouse export for the Finland Hilma source."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.finland_hilma import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME

_EXPORTS = (
    (tables.NOTICES_TABLE, tables.FI_HILMA_NOTICES_TABLE, tables.FI_HILMA_NOTICES_COLUMNS),
    (
        tables.NOTICE_WINNERS_TABLE,
        tables.FI_HILMA_NOTICE_WINNERS_TABLE,
        tables.FI_HILMA_NOTICE_WINNERS_COLUMNS,
    ),
)


def export_finland_hilma_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Replace fi_hilma_notices and fi_hilma_notice_winners from DuckDB."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.FINLAND_HILMA_DATABASE,
        tables=tuple(ch_table for _, ch_table, _ in _EXPORTS),
    )
    counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for duckdb_table, ch_table, columns in _EXPORTS:
            if log is not None:
                log("Exporting Finland Hilma table to ClickHouse: %s", ch_table)
            counts[ch_table] = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=DLT_DATASET_NAME,
                duckdb_table=duckdb_table,
                clickhouse_database=tables.FINLAND_HILMA_DATABASE,
                clickhouse_table=ch_table,
                columns=columns,
                truncate=True,
            )
    if log is not None:
        log("Finished Finland Hilma ClickHouse export: %s", counts)
    return counts
