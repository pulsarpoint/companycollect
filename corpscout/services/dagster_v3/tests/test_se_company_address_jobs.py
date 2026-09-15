"""Address source sync, global processing and their dependency order."""

import dagster as dg

from dagster_v3.defs.se_company.address import assets
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.definitions import defs as load_defs


def test_only_two_global_address_jobs_are_registered() -> None:
    repo = load_defs().get_repository_def()
    assert {job.name for job in repo.get_all_jobs() if job.name.startswith("se_company_address_")} == {
        "se_company_address_sync_job", "se_company_address_refresh_job",
    }
    assert not any(schedule.name.startswith("se_company_address_") for schedule in repo.schedule_defs)
    sync = repo.get_job("se_company_address_sync_job")
    refresh = repo.get_job("se_company_address_refresh_job")
    assert {key.path[-1] for key in sync.asset_layer.executable_asset_keys} == {
        *assets.EXTRACTOR_ASSET_NAMES, "se_company_address_normalize",
    }
    assert {key.path[-1] for key in refresh.asset_layer.executable_asset_keys} == {
        *assets.EXTRACTOR_ASSET_NAMES, "se_company_address_normalize", "se_address_geocodes_warm", "se_company_address_publish",
    }
    graph = repo.asset_graph
    normalized = graph.get(dg.AssetKey("se_company_address_normalize"))
    assert normalized.parent_keys == {dg.AssetKey(name) for name in assets.EXTRACTOR_ASSET_NAMES}
    warm = graph.get(dg.AssetKey("se_address_geocodes_warm"))
    assert dg.AssetKey("se_company_address_normalize") in warm.parent_keys
    publish = graph.get(dg.AssetKey("se_company_address_publish"))
    assert publish.parent_keys == {dg.AssetKey("se_company_address_normalize"), dg.AssetKey("se_address_geocodes_warm")}
    assert publish.partitions_def is None
    assert publish.pools == warm.pools == {osm_tables.DUCKDB_POOL}
    assert set(assets.se_company_address_publish.required_resource_keys) >= {"clickhouse", "sweden_address_osm_duckdb"}
    assert assets.se_address_geocodes_osm_snapshot_freshness_check.node_def.name in {
        node.name for node in refresh.graph.nodes
    }

    config = {"ops": {
        **{name: {"config": {"execute": True, "page_size": 10000}} for name in assets.EXTRACTOR_ASSET_NAMES},
        "se_company_address_normalize": {"config": {"changed_only": True, "page_size": 20000}},
        "se_address_geocodes_warm": {"config": {"chunk_size": 150000, "limit": 0}},
        "se_company_address_publish": {"config": {"changed_only": True, "page_size": 20000}},
    }}
    dg.validate_run_config(refresh, config)
    dg.validate_run_config(sync, {"ops": {name: value for name, value in config["ops"].items() if name not in {
        "se_address_geocodes_warm", "se_company_address_publish",
    }}})
