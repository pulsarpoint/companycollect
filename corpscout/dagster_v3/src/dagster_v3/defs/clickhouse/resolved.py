from __future__ import annotations

import uuid
from collections.abc import Sequence
from contextlib import suppress
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
    duckdb_columns = ", ".join(_quote_duckdb_identifier(column) for column in columns)
    duckdb_qualified_table = (
        f"{_quote_duckdb_identifier(duckdb_schema)}.{_quote_duckdb_identifier(duckdb_table)}"
    )
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            f"select {duckdb_columns} from {duckdb_qualified_table}"
        ).fetchall()

    if not rows and not truncate:
        return 0

    clickhouse_columns = ", ".join(_quote_clickhouse_identifier(column) for column in columns)
    clickhouse_qualified_table = (
        f"{_quote_clickhouse_identifier(clickhouse_database)}."
        f"{_quote_clickhouse_identifier(clickhouse_table)}"
    )
    if not truncate:
        clickhouse_client.execute(
            f"INSERT INTO {clickhouse_qualified_table} ({clickhouse_columns}) VALUES",
            rows,
        )
        return len(rows)

    clickhouse_stage_table = _clickhouse_stage_table_name(clickhouse_table)
    clickhouse_qualified_stage_table = _quote_clickhouse_qualified_table(
        clickhouse_database,
        clickhouse_stage_table,
    )
    clickhouse_client.execute(
        f"CREATE TABLE {clickhouse_qualified_stage_table} AS {clickhouse_qualified_table}"
    )
    try:
        if rows:
            clickhouse_client.execute(
                f"INSERT INTO {clickhouse_qualified_stage_table} ({clickhouse_columns}) VALUES",
                rows,
            )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {clickhouse_qualified_stage_table} AND {clickhouse_qualified_table}"
        )
    finally:
        with suppress(Exception):
            clickhouse_client.execute(
                f"DROP TABLE IF EXISTS {clickhouse_qualified_stage_table}"
            )
    return len(rows)


def replace_duckdb_tables_in_clickhouse(
    *,
    duckdb_path: str | Path,
    clickhouse_client: Any,
    duckdb_schema: str,
    clickhouse_database: str,
    tables: Sequence[tuple[str, Sequence[str]]],
) -> dict[str, int]:
    duckdb_path = Path(duckdb_path)
    requested_tables = tuple(tables)
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        exports = [
            (
                clickhouse_table,
                tuple(columns),
                connection.execute(
                    "select "
                    + ", ".join(_quote_duckdb_identifier(column) for column in columns)
                    + " from "
                    + f"{_quote_duckdb_identifier(duckdb_schema)}."
                    + f"{_quote_duckdb_identifier(clickhouse_table)}"
                ).fetchall(),
            )
            for clickhouse_table, columns in requested_tables
        ]

    clickhouse_columns_by_table = {
        clickhouse_table: ", ".join(_quote_clickhouse_identifier(column) for column in columns)
        for clickhouse_table, columns, _ in exports
    }
    clickhouse_qualified_tables = {
        clickhouse_table: _quote_clickhouse_qualified_table(clickhouse_database, clickhouse_table)
        for clickhouse_table, _, _ in exports
    }
    clickhouse_qualified_stage_tables: dict[str, str] = {}
    created_stage_tables: list[str] = []
    exchanged_tables: list[str] = []

    try:
        for clickhouse_table, _, _ in exports:
            clickhouse_stage_table = _clickhouse_stage_table_name(clickhouse_table)
            clickhouse_qualified_stage_table = _quote_clickhouse_qualified_table(
                clickhouse_database,
                clickhouse_stage_table,
            )
            clickhouse_qualified_stage_tables[clickhouse_table] = clickhouse_qualified_stage_table
            created_stage_tables.append(clickhouse_table)
            clickhouse_client.execute(
                f"CREATE TABLE {clickhouse_qualified_stage_table} AS "
                f"{clickhouse_qualified_tables[clickhouse_table]}"
            )

        for clickhouse_table, _, rows in exports:
            if rows:
                clickhouse_client.execute(
                    "INSERT INTO "
                    f"{clickhouse_qualified_stage_tables[clickhouse_table]} "
                    f"({clickhouse_columns_by_table[clickhouse_table]}) VALUES",
                    rows,
                )

        for clickhouse_table, _, _ in exports:
            clickhouse_client.execute(
                f"EXCHANGE TABLES {clickhouse_qualified_stage_tables[clickhouse_table]} "
                f"AND {clickhouse_qualified_tables[clickhouse_table]}"
            )
            exchanged_tables.append(clickhouse_table)
    except Exception:
        for clickhouse_table in reversed(exchanged_tables):
            with suppress(Exception):
                clickhouse_client.execute(
                    f"EXCHANGE TABLES {clickhouse_qualified_stage_tables[clickhouse_table]} "
                    f"AND {clickhouse_qualified_tables[clickhouse_table]}"
                )
        raise
    finally:
        for clickhouse_table in reversed(created_stage_tables):
            with suppress(Exception):
                clickhouse_client.execute(
                    f"DROP TABLE IF EXISTS {clickhouse_qualified_stage_tables[clickhouse_table]}"
                )

    return {clickhouse_table: len(rows) for clickhouse_table, _, rows in exports}


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _quote_clickhouse_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _quote_clickhouse_qualified_table(database: str, table: str) -> str:
    return (
        f"{_quote_clickhouse_identifier(database)}."
        f"{_quote_clickhouse_identifier(table)}"
    )


def _clickhouse_stage_table_name(table: str) -> str:
    return f"_tmp_{table}_{uuid.uuid4().hex}"
