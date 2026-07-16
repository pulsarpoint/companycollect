from datetime import date, datetime

import dagster as dg
import pytest

from dagster_v3.defs.denmark_cvr.filters import (
    DATACVR_MUNICIPALITIES,
    DATACVR_REGIONS,
    DenmarkCvrQueryFilter,
    filters_for_date_range,
)
from dagster_v3.defs.denmark_cvr.partitions import (
    DENMARK_CVR_ACTIVE_PARTITIONS,
    DENMARK_CVR_BACKFILL_PARTITIONS,
    active_partition_date,
    backfill_month_date_range,
)


def test_source_filter_lists_contain_every_region_and_municipality() -> None:
    assert DATACVR_REGIONS == [
        ("0", "Grønland"),
        ("29190623", "Region Hovedstaden"),
        ("29190925", "Region Midtjylland"),
        ("29190941", "Region Nordjylland"),
        ("29190658", "Region Sjælland"),
        ("29190909", "Region Syddanmark"),
    ]
    assert len(DATACVR_MUNICIPALITIES) == 105
    assert len({municipality for _, municipality, _ in DATACVR_MUNICIPALITIES}) == 105
    assert {region for region, _ in DATACVR_REGIONS} == {
        region for region, _, _ in DATACVR_MUNICIPALITIES
    }
    assert ("29190623", "101", "København") in DATACVR_MUNICIPALITIES
    assert ("0", "960", "Avannaata") in DATACVR_MUNICIPALITIES


def test_filters_for_small_date_range_use_one_generic_query() -> None:
    filters = filters_for_date_range(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        advertised_count=3_000,
    )

    assert filters == (
        DenmarkCvrQueryFilter(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            region="",
            municipality="",
        ),
    )
    assert filters[0].filter_id == "all-companies"


def test_filters_for_large_date_range_use_fixed_region_municipality_queries() -> None:
    filters = filters_for_date_range(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        advertised_count=3_001,
    )

    assert len(filters) == 105
    assert {
        (query_filter.region, query_filter.municipality) for query_filter in filters
    } == {(region, municipality) for region, municipality, _ in DATACVR_MUNICIPALITIES}
    assert all(query_filter.start_date == date(2025, 1, 1) for query_filter in filters)
    assert all(query_filter.end_date == date(2025, 1, 31) for query_filter in filters)
    assert filters[0].filter_id.startswith("region-")


def test_backfill_partitions_cover_january_2015_through_june_2026() -> None:
    assert isinstance(DENMARK_CVR_BACKFILL_PARTITIONS, dg.MonthlyPartitionsDefinition)
    partition_keys = DENMARK_CVR_BACKFILL_PARTITIONS.get_partition_keys(
        current_time=datetime(2027, 1, 1)
    )

    assert partition_keys[0] == "2015-01"
    assert partition_keys[-1] == "2026-06"
    assert len(partition_keys) == 138
    assert "2026-07" not in partition_keys
    assert backfill_month_date_range("2016-02") == (
        date(2016, 2, 1),
        date(2016, 2, 29),
    )

    with pytest.raises(ValueError):
        backfill_month_date_range("2025-1")
    with pytest.raises(ValueError):
        backfill_month_date_range("2026-07")


def test_active_partitions_start_on_july_1_2026_and_include_current_day() -> None:
    assert isinstance(DENMARK_CVR_ACTIVE_PARTITIONS, dg.DailyPartitionsDefinition)
    partition_keys = DENMARK_CVR_ACTIVE_PARTITIONS.get_partition_keys(
        current_time=datetime(2026, 7, 4, 12, 0)
    )

    assert partition_keys == [
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
    ]
    assert active_partition_date("2026-07-01") == date(2026, 7, 1)

    with pytest.raises(ValueError):
        active_partition_date("2026-06-30")
    with pytest.raises(ValueError):
        active_partition_date("2026-7-01")
