import tempfile
from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import tables
from dagster_v3.defs.latvia_ur.resources import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    HttpSession,
    _download_to_path,
)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def load_latvia_ur_financial_csv(
    *,
    database_path: str | Path,
    download_url: str,
    raw_table: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
) -> int:
    """Download one financial CSV and (re)load it into a DuckDB raw staging table.

    Uses DuckDB read_csv with all_varchar so the large flat files load fast and
    losslessly; the pivot step casts numerics. Each raw table is an independent
    checkpoint, so a failure in one file never re-downloads the others.
    """
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="latvia_ur_fin_") as tmpdir:
        csv_path = Path(tmpdir) / f"{raw_table}.csv"
        _download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        with duckdb.connect(str(database_path)) as connection:
            connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
            connection.execute(
                f"create or replace table {DLT_DATASET_NAME}.{raw_table} as "
                "select * from read_csv(?, delim=';', header=true, all_varchar=true, "
                "quote='\"', escape='\"')",
                [str(csv_path)],
            )
            count = connection.execute(
                f"select count(*) from {DLT_DATASET_NAME}.{raw_table}"
            ).fetchone()[0]
    return int(count)
