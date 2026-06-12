import dagster as dg


def test_definitions_load():
    from dagster_corpscout.definitions import defs

    assets_def = defs.get_assets_def(dg.AssetKey(["finland_prhytj", "raw_snapshot"]))
    assert assets_def is not None


def test_definitions_include_finland_prhytj_import_assets():
    from dagster_corpscout.definitions import defs

    assert defs.get_assets_def(dg.AssetKey(["finland_prhytj", "normalized_tables"])) is not None
    assert defs.get_assets_def(dg.AssetKey(["finland_prhytj", "code_lists"])) is not None
    assert defs.get_assets_def(dg.AssetKey(["finland_prhytj", "industry_nace_mappings"])) is not None
    assert defs.get_assets_def(dg.AssetKey(["finland_prhytj", "company_explorer_cache"])) is not None


def test_pull_schedule_exists_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.resolve_schedule_def("finland_prhytj_pull_schedule")
    assert schedule.cron_schedule == "0 3 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "finland_prhytj_pipeline"
