"""The person extract job and its STOPPED weekly (spec 2026-09-09 section 6), plus the
normalize asset's dependence on every extractor."""

import dagster as dg

from dagster_v3.defs.se_company.person import assets, jobs


def _repo():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_the_job_selects_every_extractor_and_the_normalize_asset() -> None:
    job = _repo().get_job("se_company_person_extract_job")
    selected = {key.path[-1] for key in job.asset_layer.executable_asset_keys}
    assert selected == {*assets.EXTRACTOR_ASSET_NAMES, "se_company_person_normalize"}


def test_the_weekly_is_stopped_on_a_minute_hour_no_other_schedule_uses() -> None:
    repo = _repo()
    schedule = repo.get_schedule_def("se_company_person_weekly")
    # Spec section 6 says Monday 07:15 UTC; 07:15 is taken by
    # france_sirene_register_schedule and tests/test_schedule_cron_contracts.py forbids a
    # shared (minute, hour), so this entity took the next free minute of the same hour.
    assert schedule.cron_schedule == "25 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "se_company_person_extract_job"
    taken = {
        (other.cron_schedule.split()[0], other.cron_schedule.split()[1])
        for other in repo.schedule_defs
        if other.name != "se_company_person_weekly" and isinstance(other.cron_schedule, str)
    }
    assert ("25", "7") not in taken


def test_the_weekly_runs_every_extractor_and_the_normalizer_with_the_page_size() -> None:
    ops = jobs.WEEKLY_RUN_CONFIG["ops"]
    for name in assets.EXTRACTOR_ASSET_NAMES:
        assert ops[name] == {"config": {"execute": True, "page_size": jobs.WEEKLY_PAGE_SIZE}}
    assert ops["se_company_person_normalize"] == {"config": {"changed_only": True}}
    # Every person page select binds %(company_ids)s twice and the helper runs it under
    # max_query_size 1 MiB; 10,000 twelve-digit ids render to about 130 KB per binding.
    assert jobs.WEEKLY_PAGE_SIZE == 10_000


def test_the_normalize_asset_runs_after_the_extractors() -> None:
    node = _repo().asset_graph.get(dg.AssetKey("se_company_person_normalize"))
    assert {k.path[-1] for k in node.parent_keys} == set(assets.EXTRACTOR_ASSET_NAMES)


def test_no_person_sensor_yet() -> None:
    assert not any("se_company_person" in sensor.name for sensor in _repo().sensor_defs)
