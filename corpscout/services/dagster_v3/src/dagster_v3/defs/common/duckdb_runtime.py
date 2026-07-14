from __future__ import annotations

import os
from pathlib import Path

import duckdb

DEFAULT_DUCKDB_THREADS = "4"
DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE = "100GiB"


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    clean_value = value.strip()
    return clean_value if clean_value else None


def apply_duckdb_runtime_settings(
    connection: duckdb.DuckDBPyConnection,
    *,
    default_temp_directory: str | Path,
) -> None:
    memory_limit = _env_value("DUCKDB_MEMORY_LIMIT")
    temp_directory = Path(
        _env_value("DUCKDB_TEMP_DIRECTORY") or default_temp_directory
    )
    max_temp_directory_size = (
        _env_value("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
        or DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE
    )
    threads = _env_value("DUCKDB_THREADS") or DEFAULT_DUCKDB_THREADS

    temp_directory.mkdir(parents=True, exist_ok=True)
    connection.execute("set preserve_insertion_order = false")
    connection.execute(f"set temp_directory = {_sql_literal(temp_directory)}")
    connection.execute(
        f"set max_temp_directory_size = {_sql_literal(max_temp_directory_size)}"
    )
    connection.execute(f"set threads = {int(threads)}")
    if memory_limit is not None:
        connection.execute(f"set memory_limit = {_sql_literal(memory_limit)}")
