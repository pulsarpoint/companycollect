from calendar import monthrange
from datetime import date, datetime

import dagster as dg

DENMARK_CVR_FIRST_MONTH = date(2015, 1, 1)
DENMARK_CVR_MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2015-01",
    timezone="Europe/Copenhagen",
    fmt="%Y-%m",
)


def month_date_range(partition_key: str) -> tuple[date, date]:
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
    return (
        start_date,
        date(
            start_date.year,
            start_date.month,
            monthrange(start_date.year, start_date.month)[1],
        ),
    )
