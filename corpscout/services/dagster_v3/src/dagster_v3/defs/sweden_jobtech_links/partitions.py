from dataclasses import dataclass
from datetime import date, timedelta

import dagster as dg

PARTITIONS_NAME = "sweden_jobtech_links_snapshot_windows"
DAILY_CUTOVER_DATE = date(2026, 9, 1)
YEAR_PARTITION_KEYS = tuple(f"year:{year}" for year in range(2021, 2026))
MONTH_PARTITION_KEYS = tuple(f"month:2026-{month:02d}" for month in range(1, 9))
BACKFILL_PARTITION_KEYS = YEAR_PARTITION_KEYS + MONTH_PARTITION_KEYS

SNAPSHOT_PARTITIONS = dg.DynamicPartitionsDefinition(name=PARTITIONS_NAME)


@dataclass(frozen=True)
class ArchiveWindow:
    kind: str
    value: str
    start: date
    end_exclusive: date


@dataclass(frozen=True)
class CatalogPartitionPlan:
    partition_keys_to_add: tuple[str, ...]
    daily_partition_keys_to_run: tuple[str, ...]


def archive_window(partition_key: str) -> ArchiveWindow:
    if partition_key in YEAR_PARTITION_KEYS:
        year = int(partition_key.removeprefix("year:"))
        return ArchiveWindow(
            kind="year",
            value=str(year),
            start=date(year, 1, 1),
            end_exclusive=date(year + 1, 1, 1),
        )

    if partition_key in MONTH_PARTITION_KEYS:
        value = partition_key.removeprefix("month:")
        start = date.fromisoformat(f"{value}-01")
        end_exclusive = (
            date(start.year + 1, 1, 1)
            if start.month == 12
            else date(start.year, start.month + 1, 1)
        )
        return ArchiveWindow(
            kind="month",
            value=value,
            start=start,
            end_exclusive=end_exclusive,
        )

    if partition_key.startswith("day:"):
        value = partition_key.removeprefix("day:")
        try:
            start = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"Invalid JobTech Links partition key {partition_key!r}"
            ) from exc
        if start < DAILY_CUTOVER_DATE:
            raise ValueError(
                f"Invalid JobTech Links partition key {partition_key!r}: "
                f"daily partitions start at {DAILY_CUTOVER_DATE.isoformat()}"
            )
        return ArchiveWindow(
            kind="day",
            value=value,
            start=start,
            end_exclusive=start + timedelta(days=1),
        )

    raise ValueError(f"Invalid JobTech Links partition key {partition_key!r}")


def plan_catalog_partitions(
    *,
    available_dates: tuple[date, ...] | list[date],
    existing_partition_keys: set[str],
) -> CatalogPartitionPlan:
    unique_dates = tuple(sorted(set(available_dates)))
    desired_keys = [
        partition_key
        for partition_key in BACKFILL_PARTITION_KEYS
        if _window_contains_available_date(archive_window(partition_key), unique_dates)
    ]
    desired_keys.extend(
        f"day:{available_date.isoformat()}"
        for available_date in unique_dates
        if available_date >= DAILY_CUTOVER_DATE
    )
    keys_to_add = tuple(
        partition_key
        for partition_key in desired_keys
        if partition_key not in existing_partition_keys
    )
    return CatalogPartitionPlan(
        partition_keys_to_add=keys_to_add,
        daily_partition_keys_to_run=tuple(
            partition_key
            for partition_key in keys_to_add
            if partition_key.startswith("day:")
        ),
    )


def _window_contains_available_date(
    window: ArchiveWindow,
    available_dates: tuple[date, ...],
) -> bool:
    return any(
        window.start <= available_date < window.end_exclusive
        for available_date in available_dates
    )
