"""Windowing for Finland financial-statement updates.

PRH discovery is windowed by **registration date**, so updates are precise deltas
(no bulk archives like UK): the incremental asset processes one registration-day
per partition; the backfill asset processes one registration-month per partition.
"""

import calendar
import datetime as dt


def month_window(partition_key: str) -> tuple[str, str]:
    """(start, end) registration-date window for a YYYY-MM-01 backfill partition."""
    day = dt.date.fromisoformat(partition_key)
    last = calendar.monthrange(day.year, day.month)[1]
    return day.isoformat(), day.replace(day=last).isoformat()


def day_window(partition_key: str) -> tuple[str, str]:
    """(start, end) registration-date window for a daily incremental partition."""
    day = dt.date.fromisoformat(partition_key)
    return day.isoformat(), day.isoformat()
