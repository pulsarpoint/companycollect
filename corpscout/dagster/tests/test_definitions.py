import dagster as dg


def test_definitions_load():
    from dagster_corpscout.definitions import defs

    assets_def = defs.get_assets_def(dg.AssetKey(["finland_prhytj", "raw_snapshot"]))
    assert assets_def is not None


def test_raw_snapshot_has_external_prh_ytj_api_upstream_asset():
    from dagster_corpscout.definitions import defs

    api_key = dg.AssetKey(["finland_prhytj", "prh_ytj_open_data_api"])
    raw_key = dg.AssetKey(["finland_prhytj", "raw_snapshot"])

    graph = defs.resolve_asset_graph()
    raw_node = next(node for node in graph.asset_nodes if node.key == raw_key)

    assert api_key in graph.external_asset_keys
    assert api_key in {parent.key for parent in graph.get_parents(raw_node)}


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


def test_transform_latest_job_excludes_raw_download():
    from dagster_corpscout.definitions import defs
    from dagster_corpscout.sources.finland_prhytj.schedules import transform_latest_job

    defs.resolve_job_def("finland_prhytj_transform_latest")
    selected = set(transform_latest_job.selection.selected_keys)

    assert dg.AssetKey(["finland_prhytj", "raw_snapshot"]) not in selected
    assert selected == {
        dg.AssetKey(["finland_prhytj", "normalized_tables"]),
        dg.AssetKey(["finland_prhytj", "code_lists"]),
        dg.AssetKey(["finland_prhytj", "industry_nace_mappings"]),
        dg.AssetKey(["finland_prhytj", "company_explorer_cache"]),
    }
