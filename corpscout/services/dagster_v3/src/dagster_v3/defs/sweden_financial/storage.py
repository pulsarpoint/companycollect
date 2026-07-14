from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.common.duckdb_resources import duckdb_connection_config
from dagster_v3.defs.sweden_financial.parsing import (
    SWEDEN_FINANCIAL_DATASET_NAME,
    SWEDEN_FINANCIAL_DUCKDB_ROOT,
    sweden_financial_source_duckdb_path,
)


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
