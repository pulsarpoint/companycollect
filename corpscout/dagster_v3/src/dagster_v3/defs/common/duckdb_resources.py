from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dagster_duckdb import DuckDBResource

DEFAULT_DUCKDB_THREADS = "4"
DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE = "100GiB"
DEFAULT_DUCKDB_TEMP_DIRECTORY = Path("data/duckdb_tmp")
_RESOURCE_CACHE: dict[tuple[str, tuple[tuple[str, Any], ...]], DuckDBResource] = {}


def duckdb_resource(
    database: str | Path,
    *,
    default_temp_directory: str | Path | None = None,
) -> DuckDBResource:
    database_path = _normal_database_path(database)
    connection_config = duckdb_connection_config(
        default_temp_directory=(
            default_temp_directory
            if default_temp_directory is not None
            else database_path.parent / "duckdb_tmp"
        )
    )
    cache_key = (str(database_path), tuple(sorted(connection_config.items())))
    resource = _RESOURCE_CACHE.get(cache_key)
    if resource is None:
        resource = DuckDBResource(
            database=str(database_path),
            connection_config=connection_config,
        )
        _RESOURCE_CACHE[cache_key] = resource
    return resource


def duckdb_database_path(resource: DuckDBResource) -> Path:
    return Path(str(resource.database))


def _normal_database_path(database: str | Path) -> Path:
    database_path = Path(database).expanduser()
    if str(database_path) == ":memory:":
        return database_path
    if not database_path.is_absolute():
        database_path = database_path.resolve()
    return database_path


@contextmanager
def read_only_duckdb_connection(resource: DuckDBResource) -> Iterator[Any]:
    read_only_resource = DuckDBResource(
        database=str(duckdb_database_path(resource)),
        connection_config={**resource.connection_config, "access_mode": "READ_ONLY"},
    )
    with read_only_resource.get_connection() as connection:
        yield connection


def duckdb_connection_config(
    *,
    default_temp_directory: str | Path = DEFAULT_DUCKDB_TEMP_DIRECTORY,
) -> dict[str, Any]:
    temp_directory = Path(_env_value("DUCKDB_TEMP_DIRECTORY") or default_temp_directory)

    config: dict[str, Any] = {
        "temp_directory": str(temp_directory),
        "max_temp_directory_size": (
            _env_value("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
            or DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE
        ),
        "threads": _env_value("DUCKDB_THREADS") or DEFAULT_DUCKDB_THREADS,
        "preserve_insertion_order": _env_bool(
            "DUCKDB_PRESERVE_INSERTION_ORDER",
            default=False,
        ),
    }
    memory_limit = _env_value("DUCKDB_MEMORY_LIMIT")
    if memory_limit is not None:
        config["memory_limit"] = memory_limit
    return config


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    clean_value = value.strip()
    return clean_value if clean_value else None


def _env_bool(name: str, *, default: bool) -> bool:
    value = _env_value(name)
    if value is None:
        return default
    normalized = value.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false, yes/no, on/off, or 1/0")
