"""Wiring of the address fold assets: partitions, the OSM workbench pool, config bounds."""

from types import SimpleNamespace

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


def test_the_warm_asset_carries_the_workbench_pool_and_resources() -> None:
    warm = assets.se_address_geocodes_warm
    assert warm.op.pool == osm_tables.DUCKDB_POOL
    assert set(warm.required_resource_keys) >= {"clickhouse", "sweden_address_osm_duckdb"}
    assert warm.group_names_by_key[warm.key] == assets.GROUP_NAME


def test_the_warm_asset_depends_on_the_osm_extract() -> None:
    warm = assets.se_address_geocodes_warm
    assert {dep.asset_key for dep in warm.specs_by_key[warm.key].deps} == {
        dg.AssetKey("sweden_osm_addresses_duckdb")
    }


def test_warm_config_defaults_and_bounds() -> None:
    assert assets.AddressWarmConfig().chunk_size == assets.WARM_CHUNK_SIZE == 150_000
    assert assets.AddressWarmConfig().limit == 0
    with pytest.raises(ValueError):
        assets.AddressWarmConfig(chunk_size=1)


def test_targeted_fold_normalizes_the_ids_before_folding_them(monkeypatch) -> None:
    from datetime import UTC, datetime

    calls: list[tuple] = []

    def fake_normalize(client, ids, *, changed_only, normalized_at, page_size, log):
        calls.append(("normalize", list(ids), changed_only))
        return SimpleNamespace(as_metadata=lambda: {"rows": 3})

    def fake_fold(client, duckdb, ids, *, changed_only, source_run_id, folded_at, page_size, log):
        calls.append(("fold", list(ids), changed_only, source_run_id))
        return SimpleNamespace(as_metadata=lambda: {"published": 2})

    monkeypatch.setattr(assets, "normalize_companies", fake_normalize)
    monkeypatch.setattr(assets, "fold_companies", fake_fold)
    now = datetime(2026, 9, 7, 20, 0, tzinfo=UTC)
    normalized, folded = assets.targeted_fold(
        object(), object(), ["5560000001"], changed_only=False, source_run_id="run-1", folded_at=now,
        page_size=20_000, log=None,
    )
    assert calls == [("normalize", ["5560000001"], True), ("fold", ["5560000001"], False, "run-1")]
    assert normalized.as_metadata() == {"rows": 3} and folded.as_metadata() == {"published": 2}
