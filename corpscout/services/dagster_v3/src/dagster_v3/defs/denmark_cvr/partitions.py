from calendar import monthrange
from datetime import date, datetime

import dagster as dg

DENMARK_CVR_FIRST_MONTH = date(2015, 1, 1)
DENMARK_CVR_BACKFILL_END_DATE = date(2026, 7, 1)
DENMARK_CVR_BACKFILL_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2015-01",
    end_date="2026-07",
    timezone="Europe/Copenhagen",
    fmt="%Y-%m",
)


def backfill_month_date_range(partition_key: str) -> tuple[date, date]:
    try:
        start_date = datetime.strptime(partition_key, "%Y-%m").date()
    except ValueError:
        raise ValueError(
            f"Invalid Denmark CVR monthly partition key: {partition_key!r}"
        ) from None
    if start_date.strftime("%Y-%m") != partition_key:
        raise ValueError(
            f"Invalid Denmark CVR monthly partition key: {partition_key!r}"
        )
    if start_date < DENMARK_CVR_FIRST_MONTH:
        raise ValueError(
            f"Denmark CVR partition must not precede {DENMARK_CVR_FIRST_MONTH:%Y-%m}"
        )
    if start_date >= DENMARK_CVR_BACKFILL_END_DATE:
        raise ValueError(
            "Denmark CVR backfill partition must precede "
            f"{DENMARK_CVR_BACKFILL_END_DATE:%Y-%m}"
        )
    return (
        start_date,
        date(
            start_date.year,
            start_date.month,
            monthrange(start_date.year, start_date.month)[1],
        ),
    )
