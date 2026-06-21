"""Cursor + windowing for Finland financial-statement updates.

PRH discovery is windowed by **registration date**, so updates are precise deltas
(no bulk archives like UK): the incremental asset fetches `(cursor, today]`; the
backfill asset processes one registration-month per partition.
"""

import calendar
import datetime as dt
from pathlib import Path

import duckdb

from dagster_v3.defs.finland_financials import tables

CURSOR_TABLE = f"{tables.DLT_DATASET_NAME}.ingest_cursor"
CURSOR_NAME = "registration_date"
DEFAULT_LOOKBACK_DAYS = 7


def _ensure_cursor_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {CURSOR_TABLE} "
        "(name varchar primary key, last_registration_date varchar)"
    )


def read_cursor(database_path: str | Path) -> str | None:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        _ensure_cursor_table(connection)
        row = connection.execute(
            f"select last_registration_date from {CURSOR_TABLE} where name = ?",
            [CURSOR_NAME],
        ).fetchone()
    return row[0] if row else None


def write_cursor(database_path: str | Path, last_registration_date: str) -> None:
    with duckdb.connect(str(database_path)) as connection:
        _ensure_cursor_table(connection)
        connection.execute(
            f"insert into {CURSOR_TABLE} values (?, ?) "
            "on conflict (name) do update set "
            "last_registration_date = excluded.last_registration_date",
            [CURSOR_NAME, last_registration_date],
        )


def incremental_window(
    cursor: str | None, today: dt.date, lookback_days: int = DEFAULT_LOOKBACK_DAYS
) -> tuple[str, str]:
    """(start, end) registration-date window for an incremental run."""
    if cursor:
        start = cursor
    else:
        start = (today - dt.timedelta(days=lookback_days)).isoformat()
    return start, today.isoformat()


def month_window(partition_key: str) -> tuple[str, str]:
    """(start, end) registration-date window for a YYYY-MM-01 backfill partition."""
    day = dt.date.fromisoformat(partition_key)
    last = calendar.monthrange(day.year, day.month)[1]
    return day.isoformat(), day.replace(day=last).isoformat()
