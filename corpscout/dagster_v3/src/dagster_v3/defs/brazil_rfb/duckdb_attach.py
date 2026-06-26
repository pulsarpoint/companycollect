from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@contextmanager
def attached_read_only_database(
    connection: duckdb.DuckDBPyConnection,
    *,
    database_path: str | Path,
    alias: str,
) -> Iterator[str]:
    if not alias.replace("_", "").isalnum():
        raise ValueError(f"DuckDB attach alias must be alphanumeric/underscore: {alias!r}")

    path = Path(database_path).expanduser()
    if not path.is_absolute():
        path = path.resolve()

    connection.execute(f"attach {sql_literal(path)} as {alias} (read_only)")
    try:
        yield alias
    finally:
        connection.execute(f"detach {alias}")
