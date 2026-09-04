from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.basic_info.jobs import (
    EXTRACTOR_ASSETS,
    LLM_EXTRACTOR_ASSET,
    SQL_EXTRACTOR_ASSETS,
)


def test_extract_job_and_stopped_weekly_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    job = repo.get_job("se_company_basic_info_extract_job")
    keys = {key.path[-1] for key in job.asset_layer.executable_asset_keys}
    assert keys == {
        "se_basic_info_suggestions_scb", "se_basic_info_suggestions_bolagsverket", "se_basic_info_suggestions_esef",
        "se_basic_info_suggestions_wikidata", "se_basic_info_suggestions_ratsit", "se_basic_info_suggestions_llm",
    }
    schedule = repo.get_schedule_def("se_company_basic_info_weekly")
    assert schedule.cron_schedule == "40 6 * * 1"
    assert schedule.job.name == "se_company_basic_info_extract_job"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    # Every extractor in the scheduled run executes for real, with the pinned model profile.
    # ScheduleDefinition doesn't expose run_config directly (only via evaluation, like the
    # daemon does) -- read it off an evaluated tick, same as se_company_address_weekly's test.
    context = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 8, 24, 6, 40, tzinfo=UTC))
    run_requests = schedule.evaluate_tick(context).run_requests
    assert run_requests is not None
    run_config = run_requests[0].run_config
    for source in ("scb", "bolagsverket", "esef", "wikidata", "ratsit"):
        # page_size 20,000 is 4x fewer scan pages than the default on the two big registers.
        assert run_config["ops"][f"se_basic_info_suggestions_{source}"]["config"] == {
            "execute": True, "page_size": 20_000,
        }
    llm = run_config["ops"]["se_basic_info_suggestions_llm"]["config"]
    assert llm["execute"] is True and llm["llm"]["provider"] == "deepseek" and llm["llm"]["model"] == "deepseek-v4-flash"
    # The scheduled LLM run is capped: if the schedule is ever started before the manual
    # backfill has converged, it must not be the thing that discovers the gate's size.
    assert llm["max_companies"] == 5_000
    graph = repo.asset_graph
    llm_node = graph.get(dg.AssetKey("se_basic_info_suggestions_llm"))
    assert {k.path[-1] for k in llm_node.parent_keys} == keys - {"se_basic_info_suggestions_llm"}
    for source in ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "llm"):
        assert graph.get(dg.AssetKey(f"se_basic_info_suggestions_{source}")).group_name == "se_company_basic_info"
    assert not any("basic_info" in s.name for s in repo.sensor_defs)
    # The SQL extractors are named, not sliced off the end of EXTRACTOR_ASSETS: adding a
    # seventh extractor must not silently hand it the llm's run config, or vice versa.
    assert LLM_EXTRACTOR_ASSET not in SQL_EXTRACTOR_ASSETS
    assert EXTRACTOR_ASSETS == (*SQL_EXTRACTOR_ASSETS, LLM_EXTRACTOR_ASSET)
