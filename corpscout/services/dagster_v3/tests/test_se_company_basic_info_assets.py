"""Spec section 6: the fold assets and the precedence export, registered and configured."""

import re
from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.assets import (
    BASIC_INFO_FOLD_PARTITIONS,
    FOLD_POOL,
    GROUP_NAME,
    BasicInfoFoldCompaniesConfig,
    BasicInfoFoldConfig,
    basic_info_bucket_index,
    export_precedence,
)
from dagster_v3.defs.se_company.basic_info.batch import BUCKET_COUNT
from dagster_v3.defs.se_company.basic_info.precedence import precedence_rows


def test_partitions_are_sixty_four_hash_buckets() -> None:
    keys = BASIC_INFO_FOLD_PARTITIONS.get_partition_keys()
    assert len(keys) == BUCKET_COUNT == 64
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63"
    assert basic_info_bucket_index("bucket_07") == 7
    assert basic_info_bucket_index("bucket_00") == 0
    assert basic_info_bucket_index("bucket_63") == 63
    with pytest.raises(ValueError):
        basic_info_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        basic_info_bucket_index("07")
    # Only the canonical two-digit key parses: a bare single digit or an extra digit
    # (that would otherwise silently truncate to a valid-looking bucket) is rejected.
    with pytest.raises(ValueError):
        basic_info_bucket_index("bucket_7")
    with pytest.raises(ValueError):
        basic_info_bucket_index("bucket_064")


def test_configs_default_the_way_the_spec_says() -> None:
    assert BasicInfoFoldConfig().changed_only is True
    targeted = BasicInfoFoldCompaniesConfig(company_ids=["5560000000"])
    assert targeted.changed_only is False
    with pytest.raises(ValueError):
        BasicInfoFoldCompaniesConfig(company_ids=[])


def test_assets_are_registered_with_pool_partitions_and_backfill_policy() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    graph = repo.asset_graph
    fold = graph.get(dg.AssetKey("se_company_basic_info_fold"))
    assert fold.group_name == GROUP_NAME
    assert fold.partitions_def == BASIC_INFO_FOLD_PARTITIONS
    assert fold.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    targeted = graph.get(dg.AssetKey("se_company_basic_info_fold_companies"))
    assert targeted.group_name == GROUP_NAME
    assert targeted.partitions_def is None
    export = graph.get(dg.AssetKey("se_company_basic_info_precedence_clickhouse"))
    assert export.group_name == GROUP_NAME
    # One pool serializes the two fold assets (tests/test_sweden_company_assets.py pins
    # pools the same way).
    assert fold.pools == {FOLD_POOL}
    assert targeted.pools == {FOLD_POOL}
    assert export.pools == set()
    # No automation in this slice: nothing schedules or senses these assets.
    for schedule in repo.schedule_defs:
        assert "basic_info" not in schedule.name
    for sensor in repo.sensor_defs:
        assert "basic_info" not in sensor.name


class _FakePrecedenceClient:
    """Records every statement/params pair; the stale-count query always returns 0
    rows changed (nothing pre-existing to go stale in this test)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql: str, params: object = None) -> list[tuple[object, ...]]:
        self.calls.append((sql, params))
        if sql.lstrip().startswith("SELECT count()"):
            return [(0,)]
        return []


def test_export_precedence_inserts_every_pair_and_binds_a_utc_millisecond_string() -> None:
    # export_precedence is factored out of se_company_basic_info_precedence_clickhouse
    # specifically so it can be driven directly against a fake client here, without
    # needing to build a Dagster asset execution context.
    client = _FakePrecedenceClient()
    exported_at = datetime(2026, 9, 3, 12, 34, 56, 789_000, tzinfo=UTC)

    pairs, stale = export_precedence(client, exported_at)

    expected_rows = precedence_rows()
    # 30 pairs as BASIC_INFO_PRECEDENCE stands today (not 33 -- counted directly from
    # precedence_rows() below so this test tracks the dictionary rather than drifting
    # from it if a source/field pair is added or removed).
    assert len(expected_rows) == 30
    assert pairs == len(expected_rows) == 30
    assert stale == 0

    assert len(client.calls) == 2
    insert_sql, insert_rows = client.calls[0]
    assert insert_sql == (
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
        f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
    )
    assert insert_sql == (
        "INSERT INTO corpscout.se_company_basic_info_precedence "
        "(field, source, precedence, exported_at) VALUES"
    )
    assert len(insert_rows) == len(expected_rows)

    stale_sql, stale_params = client.calls[1]
    assert "toDateTime64(%(exported_at)s, 3, 'UTC')" in stale_sql
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}", stale_params["exported_at"])
    assert stale_params["exported_at"] == "2026-09-03 12:34:56.789"
