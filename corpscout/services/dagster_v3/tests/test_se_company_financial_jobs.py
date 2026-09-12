"""The extract job and the stopped weekly (spec section 8)."""

import dagster as dg

from dagster_v3.defs.se_company.financial import jobs
from dagster_v3.defs.se_company.financial.assets import EXTRACTOR_ASSET_NAMES, EXTRACTOR_SOURCES
from dagster_v3.definitions import defs as load_project_defs


def test_the_extractor_names_follow_the_sources() -> None:
    assert EXTRACTOR_SOURCES == ("bolagsverket", "bolagsverket_comparative", "esef", "ratsit")
    assert EXTRACTOR_ASSET_NAMES == tuple(f"se_company_financial_suggestions_{s}" for s in EXTRACTOR_SOURCES)


def test_the_job_runs_the_ratsit_usd_step_and_the_four_extractors() -> None:
    repository = load_project_defs().get_repository_def()
    assert repository.has_job("se_company_financial_extract_job")
    job = repository.get_job("se_company_financial_extract_job")
    keys = {key.to_user_string() for key in job.asset_layer.executable_asset_keys}
    assert keys == {"se_ratsit_financial_periods_usd", *EXTRACTOR_ASSET_NAMES}
    for name in EXTRACTOR_ASSET_NAMES:
        node = repository.asset_graph.get(dg.AssetKey(name))
        assert node.group_name == "se_company_financial" and node.partitions_def is None


def test_the_weekly_is_stopped_at_a_free_minute_and_executes() -> None:
    schedule = jobs.se_company_financial_weekly
    assert schedule.cron_schedule == "55 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "se_company_financial_extract_job"
    config = jobs.WEEKLY_RUN_CONFIG["ops"]
    assert config["se_ratsit_financial_periods_usd"] == {"config": {"execute": True}}
    for name in EXTRACTOR_ASSET_NAMES:
        assert config[name] == {"config": {"execute": True, "page_size": 5000}}
