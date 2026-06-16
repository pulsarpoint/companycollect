from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
from dagster_clickhouse import ClickhouseResource

RESOLVED_DATABASE = "corpscout_resolved"

REQUIRED_FINLAND_RESOLVED_TABLES = (
    "fi_companies",
    "fi_websites",
    "fi_industries",
    "fi_addresses",
    "fi_registered_entries",
    "fi_legal_forms",
    "fi_financial_statements",
    "fi_financial_metrics",
)


def assert_clickhouse_tables_exist(
    clickhouse: ClickhouseResource,
    *,
    database: str,
    tables: Sequence[str],
) -> None:
    requested_tables = tuple(tables)
    with clickhouse.get_connection() as client:
        rows = client.execute(
            """
            SELECT name
            FROM system.tables
            WHERE database = %(database)s
              AND name IN %(tables)s
            """,
            {"database": database, "tables": requested_tables},
        )

    existing = {str(row[0]) for row in rows}
    missing = [table for table in requested_tables if table not in existing]
    if missing:
        raise ValueError(f"Missing ClickHouse tables in {database}: {', '.join(missing)}")


def export_duckdb_table_to_clickhouse(
    *,
    duckdb_path: str | Path,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    columns: Sequence[str],
    truncate: bool,
) -> int:
    if truncate:
        clickhouse_client.execute(
            f"TRUNCATE TABLE {_quote_clickhouse_identifier(clickhouse_database)}.{_quote_clickhouse_identifier(clickhouse_table)}"
        )

    duckdb_columns = ", ".join(_quote_duckdb_identifier(column) for column in columns)
    duckdb_qualified_table = (
        f"{_quote_duckdb_identifier(duckdb_schema)}.{_quote_duckdb_identifier(duckdb_table)}"
    )
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            f"select {duckdb_columns} from {duckdb_qualified_table}"
        ).fetchall()

    if not rows:
        return 0

    clickhouse_columns = ", ".join(_quote_clickhouse_identifier(column) for column in columns)
    clickhouse_client.execute(
        (
            f"INSERT INTO {_quote_clickhouse_identifier(clickhouse_database)}."
            f"{_quote_clickhouse_identifier(clickhouse_table)} ({clickhouse_columns}) VALUES"
        ),
        rows,
    )
    return len(rows)


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _quote_clickhouse_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"
