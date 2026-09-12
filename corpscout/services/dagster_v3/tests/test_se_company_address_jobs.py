"""The address extract job and its STOPPED weekly (spec section 7), plus the normalize
asset's dependence on the four extractors."""

import dagster as dg

from dagster_v3.defs.se_company.address import assets, jobs


def _repo():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_the_job_selects_the_three_extractors_and_the_normalize_asset() -> None:
    job = _repo().get_job("se_company_address_extract_job")
    selected = {key.path[-1] for key in job.asset_layer.executable_asset_keys}
    assert selected == {*assets.EXTRACTOR_ASSET_NAMES, "se_company_address_normalize"}


def test_the_weekly_is_registered_stopped_with_execute_and_the_page_size() -> None:
    """The `_v2` interim name existed only to avoid colliding with the old model's schedule,
    which retired in slice 4b; the extract weekly now carries the canonical name. It stays
    STOPPED, and it stays extract + normalize only -- the fold is manual by design."""
    schedule = _repo().get_schedule_def("se_company_address_weekly")
    assert schedule.cron_schedule == "5 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "se_company_address_extract_job"
    assert not any(s.name == "se_company_address_v2_weekly" for s in _repo().schedule_defs)
    ops = jobs.WEEKLY_RUN_CONFIG["ops"]
    for name in assets.EXTRACTOR_ASSET_NAMES:
        assert ops[name] == {"config": {"execute": True, "page_size": jobs.WEEKLY_PAGE_SIZE}}
    assert ops["se_company_address_normalize"] == {"config": {"changed_only": True}}
    assert jobs.WEEKLY_PAGE_SIZE == 10_000


def test_the_normalize_asset_runs_after_the_extractors() -> None:
    node = _repo().asset_graph.get(dg.AssetKey("se_company_address_normalize"))
    assert {k.path[-1] for k in node.parent_keys} == set(assets.EXTRACTOR_ASSET_NAMES)


def test_no_sensor_for_addresses_yet() -> None:
    assert not any("se_company_address_suggest" in s.name or "address_normalize" in s.name for s in _repo().sensor_defs)
