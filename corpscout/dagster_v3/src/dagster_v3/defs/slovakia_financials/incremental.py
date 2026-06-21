"""Statement-id cursor for the RÚZ forward sweep.

The RÚZ statement feed is paged by `pokracovat-za-id` (ascending statement id),
so a single id cursor walks ALL history in bounded chunks and then naturally
picks up new filings (which get higher ids). No partitions needed.
"""

from pathlib import Path

import duckdb

from dagster_v3.defs.slovakia_financials import tables

CURSOR_TABLE = f"{tables.DLT_DATASET_NAME}.ingest_cursor"
CURSOR_NAME = "statement_id"


def _ensure_cursor_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {CURSOR_TABLE} "
        "(name varchar primary key, last_id bigint)"
    )


def read_cursor(database_path: str | Path) -> int:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        _ensure_cursor_table(connection)
        row = connection.execute(
            f"select last_id from {CURSOR_TABLE} where name = ?", [CURSOR_NAME]
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def write_cursor(database_path: str | Path, last_id: int) -> None:
    with duckdb.connect(str(database_path)) as connection:
        _ensure_cursor_table(connection)
        connection.execute(
            f"insert into {CURSOR_TABLE} values (?, ?) "
            "on conflict (name) do update set last_id = excluded.last_id",
            [CURSOR_NAME, int(last_id)],
        )
