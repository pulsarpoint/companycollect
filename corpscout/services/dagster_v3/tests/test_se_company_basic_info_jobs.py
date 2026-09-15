import dagster as dg

from dagster_v3.defs.se_company.basic_info.assets import EXTRACTOR_ASSETS, FOLD_POOL, SQL_EXTRACTOR_ASSETS
from dagster_v3.definitions import defs as load_defs


def test_two_global_info_jobs_and_no_redundant_extract_job_or_schedule() -> None:
    repo = load_defs().get_repository_def()
    sync = repo.get_job("se_company_basic_info_sync_job")
    refresh = repo.get_job("se_company_basic_info_refresh_job")
    assert {key.to_user_string() for key in sync.asset_layer.executable_asset_keys} == set(SQL_EXTRACTOR_ASSETS)
    assert {key.to_user_string() for key in refresh.asset_layer.executable_asset_keys} == {
        *EXTRACTOR_ASSETS, "se_company_basic_info_publish",
    }
    assert not repo.has_job("se_company_basic_info_extract_job")
    assert not any("basic_info" in schedule.name for schedule in repo.schedule_defs)
    assert not any("basic_info" in sensor.name for sensor in repo.sensor_defs)
    graph = repo.asset_graph
    publish = graph.get(dg.AssetKey("se_company_basic_info_publish"))
    assert publish.partitions_def is None
    assert publish.pools == {FOLD_POOL}
    assert publish.parent_keys == {dg.AssetKey(name) for name in EXTRACTOR_ASSETS}
    llm = graph.get(dg.AssetKey("se_basic_info_suggestions_llm"))
    assert llm.parent_keys == {dg.AssetKey(name) for name in SQL_EXTRACTOR_ASSETS}
    assert llm.pools == {"se_basic_info_suggestions_llm"}
