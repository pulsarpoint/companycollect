"""Spec section 6: the fold assets and the precedence export, registered and configured."""

import dagster as dg
import pytest

from dagster_v3.defs.se_company.basic_info.assets import (
    BASIC_INFO_FOLD_PARTITIONS,
    FOLD_POOL,
    GROUP_NAME,
    BasicInfoFoldCompaniesConfig,
    BasicInfoFoldConfig,
    basic_info_bucket_index,
)
from dagster_v3.defs.se_company.basic_info.batch import BUCKET_COUNT


def test_partitions_are_sixty_four_hash_buckets() -> None:
    keys = BASIC_INFO_FOLD_PARTITIONS.get_partition_keys()
    assert len(keys) == BUCKET_COUNT == 64
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63"
    assert basic_info_bucket_index("bucket_07") == 7
    with pytest.raises(ValueError):
        basic_info_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        basic_info_bucket_index("07")


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
