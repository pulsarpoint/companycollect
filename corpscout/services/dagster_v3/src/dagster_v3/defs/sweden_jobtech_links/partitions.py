from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

import dagster as dg

PartitionKind = Literal["year", "month", "day"]

DAILY_CUTOVER_DATE = date(2026, 9, 1)
HISTORICAL_PARTITION_KEYS = tuple(str(year) for year in range(2021, 2026))
MONTHLY_2026_PARTITION_KEYS = tuple(
    f"2026-{month:02d}" for month in range(1, DAILY_CUTOVER_DATE.month)
)

HISTORICAL_PARTITIONS = dg.StaticPartitionsDefinition(list(HISTORICAL_PARTITION_KEYS))
MONTHLY_2026_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2026-01",
    end_date=DAILY_CUTOVER_DATE.strftime("%Y-%m"),
    timezone="UTC",
    fmt="%Y-%m",
)
DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date=DAILY_CUTOVER_DATE.isoformat(),
    timezone="UTC",
)


@dataclass(frozen=True)
class ArchiveWindow:
    kind: PartitionKind
    value: str
    start: date
    end_exclusive: date


def archive_window(partition_kind: str, partition_key: str) -> ArchiveWindow:
    if partition_kind == "year" and partition_key in HISTORICAL_PARTITION_KEYS:
        year = int(partition_key)
        return ArchiveWindow(
            kind="year",
            value=partition_key,
            start=date(year, 1, 1),
            end_exclusive=date(year + 1, 1, 1),
        )

    if partition_kind == "month" and partition_key in MONTHLY_2026_PARTITION_KEYS:
        start = date.fromisoformat(f"{partition_key}-01")
        return ArchiveWindow(
            kind="month",
            value=partition_key,
            start=start,
            end_exclusive=date(start.year, start.month + 1, 1),
        )

    if partition_kind == "day":
        try:
            start = date.fromisoformat(partition_key)
        except ValueError as exc:
            raise ValueError(
                f"Invalid JobTech Links day partition key {partition_key!r}"
            ) from exc
        if start.isoformat() == partition_key and start >= DAILY_CUTOVER_DATE:
            return ArchiveWindow(
                kind="day",
                value=partition_key,
                start=start,
                end_exclusive=start + timedelta(days=1),
            )

    raise ValueError(
        f"Invalid JobTech Links {partition_kind!r} partition key {partition_key!r}"
    )


def daily_partition_keys_from_catalog(
    available_dates: tuple[date, ...] | list[date],
) -> tuple[str, ...]:
    return tuple(
        available_date.isoformat()
        for available_date in sorted(set(available_dates))
        if available_date >= DAILY_CUTOVER_DATE
    )
