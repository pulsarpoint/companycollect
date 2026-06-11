import dagster as dg


def test_definitions_load():
    from dagster_corpscout.definitions import defs

    assets_def = defs.get_assets_def(dg.AssetKey(["finland_prhytj", "raw_snapshot"]))
    assert assets_def is not None


def test_pull_schedule_exists_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.resolve_schedule_def("finland_prhytj_pull_schedule")
    assert schedule.cron_schedule == "0 3 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
