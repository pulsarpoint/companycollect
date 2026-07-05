from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.brazil_financial.cvm.parsing import BRAZIL_CVM_DUCKDB_SCHEMA
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_connection_config,
    duckdb_resource,
)

BRAZIL_FIN_CVM_DUCKDB_ROOT = Path("data/brazil_cvm")
BRAZIL_FIN_CVM_COMPANIES_DUCKDB_PATH = BRAZIL_FIN_CVM_DUCKDB_ROOT / "companies.duckdb"
BRAZIL_FIN_CVM_SOURCE_FAMILIES = frozenset({"dfp", "itr"})


def brazil_fin_cvm_source_duckdb_path(
    *,
    family: str,
    year: str | int,
    root: str | Path = BRAZIL_FIN_CVM_DUCKDB_ROOT,
) -> Path:
    normalized_family = _normalize_family(family)
    normalized_year = _normalize_year(year)
    return Path(root) / normalized_family / f"year={normalized_year}" / "source.duckdb"


@contextmanager
def brazil_fin_cvm_source_duckdb_connection(
    *,
    family: str,
    year: str | int,
    root: str | Path = BRAZIL_FIN_CVM_DUCKDB_ROOT,
) -> Iterator[Any]:
    db_path = brazil_fin_cvm_source_duckdb_path(
        family=family,
        year=year,
        root=root,
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb_resource(db_path).get_connection() as connection:
        yield connection


@contextmanager
def brazil_fin_cvm_existing_source_duckdb_connection(
    *,
    family: str,
    year: str | int,
    root: str | Path = BRAZIL_FIN_CVM_DUCKDB_ROOT,
) -> Iterator[Any]:
    normalized_family = _normalize_family(family)
    normalized_year = _normalize_year(year)
    db_path = brazil_fin_cvm_source_duckdb_path(
        family=normalized_family,
        year=normalized_year,
        root=root,
    )
    if not db_path.exists():
        raise FileNotFoundError(
            f"Brazil CVM {normalized_family.upper()} DuckDB partition file is missing "
            f"for year {normalized_year}: {db_path}. Materialize "
            f"brazil_fin_cvm_{normalized_family}_raw_duckdb for this partition before "
            "running USD conversion."
        )
    with duckdb_resource(db_path).get_connection() as connection:
        yield connection


def existing_brazil_fin_cvm_source_duckdb_paths(
    *,
    family: str,
    years: Sequence[str | int],
    root: str | Path = BRAZIL_FIN_CVM_DUCKDB_ROOT,
) -> tuple[Path, ...]:
    return tuple(
        db_path
        for year in years
        if (
            db_path := brazil_fin_cvm_source_duckdb_path(
                family=family,
                year=year,
                root=root,
            )
        ).exists()
    )


@contextmanager
def brazil_fin_cvm_read_only_partitioned_connection(
    *,
    family: str,
    years: Sequence[str | int],
    table_names: Sequence[str],
    root: str | Path = BRAZIL_FIN_CVM_DUCKDB_ROOT,
) -> Iterator[Any]:
    normalized_family = _normalize_family(family)
    sources = tuple(
        (_normalize_year(year), db_path)
        for year in years
        if (
            db_path := brazil_fin_cvm_source_duckdb_path(
                family=normalized_family,
                year=year,
                root=root,
            )
        ).exists()
    )
    if not sources:
        raise FileNotFoundError(
            f"No Brazil CVM {normalized_family.upper()} DuckDB partition files found"
        )

    temp_directory = Path(root) / "duckdb_tmp"
    temp_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(
        ":memory:",
        config=duckdb_connection_config(default_temp_directory=temp_directory),
    )
    try:
        connection.execute(
            f"create schema if not exists {_quote_identifier(BRAZIL_CVM_DUCKDB_SCHEMA)}"
        )
        for year, db_path in sources:
            alias = f"{normalized_family}_{year}"
            connection.execute(
                "attach "
                f"{_string_literal(str(db_path.resolve()))} "
                f"as {_quote_identifier(alias)} (READ_ONLY)"
            )
        for table_name in table_names:
            union_sql = " union all ".join(
                (
                    f"select * from {_quote_identifier(f'{normalized_family}_{year}')}"
                    f".{_quote_identifier(BRAZIL_CVM_DUCKDB_SCHEMA)}"
                    f".{_quote_identifier(table_name)}"
                )
                for year, _ in sources
            )
            connection.execute(
                f"""
                create or replace view
                {_quote_identifier(BRAZIL_CVM_DUCKDB_SCHEMA)}.{_quote_identifier(table_name)}
                as {union_sql}
                """
            )
        yield connection
    finally:
        connection.close()


def _normalize_family(family: str) -> str:
    normalized = family.strip().lower()
    if normalized not in BRAZIL_FIN_CVM_SOURCE_FAMILIES:
        allowed = ", ".join(sorted(BRAZIL_FIN_CVM_SOURCE_FAMILIES))
        raise ValueError(f"Brazil CVM source family must be one of {allowed}")
    return normalized


def _normalize_year(year: str | int) -> str:
    normalized = str(year).strip()
    if not normalized.isdigit() or len(normalized) != 4:
        raise ValueError(f"Brazil CVM source year must be a four-digit year: {year!r}")
    return normalized


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
