from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_connection_config,
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.sweden_financial.parsing import (
    SWEDEN_FINANCIAL_DATASET_NAME,
    SWEDEN_FINANCIAL_DUCKDB_ROOT,
    sweden_financial_source_duckdb_path,
)


@contextmanager
def sweden_financial_year_duckdb_connection(
    year: str | int,
    *,
    root: str | Path = SWEDEN_FINANCIAL_DUCKDB_ROOT,
) -> Iterator[Any]:
    """Open a read-only connection to ONE partition year's Sweden financial
    DuckDB file, raising ``ValueError`` if this host does not hold that file.

    Mirrors how the parse assets resolve their own file
    (``sweden_financial_source_duckdb_path``), but scoped to a single year
    rather than the ``sweden_financial_read_only_partitioned_connection``
    union below -- the scoped ClickHouse export (see ``clickhouse.py``)
    always operates within exactly one partition's year file: the year
    itself for a backfill partition, or the current active year for a
    weekly current partition.
    """
    db_path = sweden_financial_source_duckdb_path(year, root=root)
    if not db_path.exists():
        raise ValueError(
            f"Sweden financial DuckDB file not found for partition {year}: "
            f"{db_path}. This host may not hold this partition's data -- "
            "each host exports only the partitions whose year files it has."
        )
    with read_only_duckdb_connection(duckdb_resource(db_path)) as connection:
        yield connection


def existing_sweden_financial_source_duckdb_paths(
    *,
    years: Sequence[str | int],
    root: str | Path = SWEDEN_FINANCIAL_DUCKDB_ROOT,
) -> tuple[Path, ...]:
    return tuple(
        db_path
        for year in years
        if (db_path := sweden_financial_source_duckdb_path(year, root=root)).exists()
    )


@contextmanager
def sweden_financial_read_only_partitioned_connection(
    *,
    years: Sequence[str | int],
    table_names: Sequence[str],
    root: str | Path = SWEDEN_FINANCIAL_DUCKDB_ROOT,
) -> Iterator[Any]:
    sources = tuple(
        (str(year), db_path)
        for year in years
        if (db_path := sweden_financial_source_duckdb_path(year, root=root)).exists()
    )
    if not sources:
        raise FileNotFoundError("No Sweden financial DuckDB year files found")

    temp_directory = Path(root) / "duckdb_tmp"
    temp_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(
        ":memory:",
        config=duckdb_connection_config(default_temp_directory=temp_directory),
    )
    try:
        connection.execute(
            f"create schema if not exists {_quote_identifier(SWEDEN_FINANCIAL_DATASET_NAME)}"
        )
        for year, db_path in sources:
            connection.execute(
                "attach "
                f"{_string_literal(str(db_path.resolve()))} "
                f"as {_quote_identifier(f'sweden_financial_{year}')} (READ_ONLY)"
            )
        for table_name in table_names:
            union_sql = " union all ".join(
                (
                    f"select * from {_quote_identifier(f'sweden_financial_{year}')}"
                    f".{_quote_identifier(SWEDEN_FINANCIAL_DATASET_NAME)}"
                    f".{_quote_identifier(table_name)}"
                )
                for year, _ in sources
            )
            connection.execute(
                f"""
                create or replace view
                {_quote_identifier(SWEDEN_FINANCIAL_DATASET_NAME)}.{_quote_identifier(table_name)}
                as {union_sql}
                """
            )
        yield connection
    finally:
        connection.close()


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
