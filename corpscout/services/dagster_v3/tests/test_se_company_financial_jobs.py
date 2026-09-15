import dagster as dg

from dagster_v3.defs.se_company.financial.assets import EXTRACTOR_ASSET_NAMES, EXTRACTOR_SOURCES, FOLD_POOL
from dagster_v3.definitions import defs as load_defs


def test_the_extractor_names_follow_the_sources() -> None:
    assert EXTRACTOR_SOURCES == ("bolagsverket", "bolagsverket_comparative", "esef", "ratsit")
    assert EXTRACTOR_ASSET_NAMES == tuple(f"se_company_financial_suggestions_{source}" for source in EXTRACTOR_SOURCES)


def test_two_global_financial_jobs_and_no_redundant_extract_job_or_schedule() -> None:
    repo = load_defs().get_repository_def()
    sync = repo.get_job("se_company_financial_sync_job")
    refresh = repo.get_job("se_company_financial_refresh_job")
    assert {key.to_user_string() for key in sync.asset_layer.executable_asset_keys} == {
        "se_ratsit_financial_periods_usd", *EXTRACTOR_ASSET_NAMES,
    }
    assert {key.to_user_string() for key in refresh.asset_layer.executable_asset_keys} == {
        "se_ratsit_financial_periods_usd", *EXTRACTOR_ASSET_NAMES, "se_company_financial_publish",
    }
    assert not repo.has_job("se_company_financial_extract_job")
    assert not any("se_company_financial" in schedule.name for schedule in repo.schedule_defs)
    graph = repo.asset_graph
    publish = graph.get(dg.AssetKey("se_company_financial_publish"))
    assert publish.partitions_def is None
    assert publish.pools == {FOLD_POOL}
    assert publish.parent_keys == {dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES}
    ratsit = graph.get(dg.AssetKey("se_company_financial_suggestions_ratsit"))
    assert dg.AssetKey("se_ratsit_financial_periods_usd") in ratsit.parent_keys
