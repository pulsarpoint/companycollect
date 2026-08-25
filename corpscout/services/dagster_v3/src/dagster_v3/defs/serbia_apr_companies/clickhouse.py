"""Atomic publication of APR company history and current state."""

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.serbia_apr_companies import tables

_TEMP_DUCKDB_SCHEMA = "temp.main"


def replace_serbia_apr_companies_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Stage both tables before exchange and roll back earlier exchanges on failure."""
    clickhouse_tables = tuple(tables.CLICKHOUSE_COLUMNS_BY_TABLE)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=clickhouse_tables,
    )

    _create_duckdb_export_views(duckdb_connection)
    try:
        with clickhouse.get_connection() as client:
            counts = replace_duckdb_connection_tables_in_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=_TEMP_DUCKDB_SCHEMA,
                clickhouse_database=tables.CLICKHOUSE_DATABASE,
                tables=tuple(tables.CLICKHOUSE_COLUMNS_BY_TABLE.items()),
                log=log,
            )
    finally:
        _drop_duckdb_export_views(duckdb_connection)

    if log is not None:
        log("Published Serbia APR company tables: row_counts=%s", counts)
    return counts


def _create_duckdb_export_views(duckdb_connection: Any) -> None:
    columns = ", ".join(f'"{column}"' for column in tables.COMPANY_EXPORT_COLUMNS)
    created_views: list[str] = []
    try:
        for (
            clickhouse_table,
            duckdb_table,
        ) in tables.DUCKDB_TABLE_BY_CLICKHOUSE_TABLE.items():
            duckdb_connection.execute(
                f"""
                create temporary view "{clickhouse_table}" as
                select {columns}
                from "{tables.DUCKDB_SCHEMA}"."{duckdb_table}"
                """
            )
            created_views.append(clickhouse_table)
    except Exception:
        _drop_duckdb_export_views(duckdb_connection, created_views)
        raise


def _drop_duckdb_export_views(
    duckdb_connection: Any,
    view_names: list[str] | tuple[str, ...] | None = None,
) -> None:
    target_views = view_names or tuple(tables.DUCKDB_TABLE_BY_CLICKHOUSE_TABLE)
    for view_name in reversed(target_views):
        duckdb_connection.execute(f'drop view if exists "temp"."main"."{view_name}"')
