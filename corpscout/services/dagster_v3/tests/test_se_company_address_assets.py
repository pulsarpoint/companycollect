"""Wiring of the address fold assets: partitions, the OSM workbench pool, config bounds."""

import dagster as dg
import pytest

from dagster_v3.defs.se_company.address import assets, batch
from dagster_v3.defs.sweden_address_osm import tables as osm_tables


def test_the_fold_has_sixty_four_bucket_partitions_and_the_workbench_pool() -> None:
    keys = assets.ADDRESS_FOLD_PARTITIONS.get_partition_keys()
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63" and len(keys) == batch.BUCKET_COUNT
    fold = assets.se_company_address_fold
    assert fold.partitions_def is assets.ADDRESS_FOLD_PARTITIONS
    assert fold.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    for asset in (fold, assets.se_company_address_fold_companies):
        assert asset.op.pool == osm_tables.DUCKDB_POOL
        assert set(asset.required_resource_keys) >= {"clickhouse", "sweden_address_osm_duckdb"}
        assert asset.group_names_by_key[asset.key] == assets.GROUP_NAME


def test_bucket_index_parses_and_refuses() -> None:
    assert assets.address_bucket_index("bucket_07") == 7
    with pytest.raises(ValueError):
        assets.address_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        assets.address_bucket_index("07")


def test_config_defaults_and_bounds() -> None:
    assert assets.AddressFoldConfig().changed_only is True
    assert assets.AddressFoldConfig().page_size == batch.PAGE_SIZE
    with pytest.raises(ValueError):
        assets.AddressFoldConfig(page_size=0)
    targeted = assets.AddressFoldCompaniesConfig(company_ids=["5560000002", "5560000001", "5560000001"])
    assert targeted.company_ids == ["5560000001", "5560000002"] and targeted.changed_only is False
    with pytest.raises(ValueError):
        assets.AddressFoldCompaniesConfig(company_ids=[])
