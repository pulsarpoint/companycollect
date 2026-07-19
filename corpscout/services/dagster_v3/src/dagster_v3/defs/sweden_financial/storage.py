from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.sweden_financial.parsing import (
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
    (``sweden_financial_source_duckdb_path``), but scoped to a single year:
    the scoped ClickHouse export (see ``clickhouse.py``) always operates
    within exactly one partition's year file -- the year itself for a
    backfill partition, or the current active year for a weekly current
    partition.
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
