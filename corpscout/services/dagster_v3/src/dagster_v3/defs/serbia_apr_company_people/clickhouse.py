"""Atomic DuckDB-to-ClickHouse publication for Serbia APR company people."""

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.serbia_apr_company_people import tables


def replace_serbia_apr_representatives_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Atomically replace the SP3/SP4 observation and current table pair."""
    table_names = tuple(tables.REPRESENTATIVE_COLUMNS_BY_TABLE)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=table_names,
    )
    with clickhouse.get_connection() as client:
        counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.REPRESENTATIVES_DUCKDB_SCHEMA,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            tables=tuple(tables.REPRESENTATIVE_COLUMNS_BY_TABLE.items()),
            log=log,
        )
    if log is not None:
        log("Published Serbia APR representative tables: row_counts=%s", counts)
    return counts


def replace_serbia_apr_beneficial_owners_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Atomically replace the CEV observation and current table pair."""
    table_names = tuple(tables.BENEFICIAL_OWNER_COLUMNS_BY_TABLE)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=table_names,
    )
    with clickhouse.get_connection() as client:
        counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.BENEFICIAL_OWNERS_DUCKDB_SCHEMA,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            tables=tuple(tables.BENEFICIAL_OWNER_COLUMNS_BY_TABLE.items()),
            log=log,
        )
    if log is not None:
        log("Published Serbia APR beneficial-owner tables: row_counts=%s", counts)
    return counts
