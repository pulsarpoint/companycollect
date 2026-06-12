import dagster as dg

SOURCE_PREFIX = ["sources", "finland", "prh_ytj"]


def source_key(name: str) -> dg.AssetKey:
    return dg.AssetKey([*SOURCE_PREFIX, name])


def test_definitions_load():
    from dagster_corpscout.definitions import defs

    assets_def = defs.get_assets_def(source_key("raw_snapshot"))
    assert assets_def is not None


def test_raw_snapshot_has_external_source_system_upstream_asset():
    from dagster_corpscout.definitions import defs

    api_key = source_key("source_system")
    raw_key = source_key("raw_snapshot")

    graph = defs.resolve_asset_graph()
    raw_node = next(node for node in graph.asset_nodes if node.key == raw_key)

    assert api_key in graph.external_asset_keys
    assert api_key in {parent.key for parent in graph.get_parents(raw_node)}


def test_definitions_include_finland_prhytj_import_assets():
    from dagster_corpscout.definitions import defs

    assert defs.get_assets_def(source_key("normalized_tables")) is not None
    assert defs.get_assets_def(source_key("code_lists")) is not None
    assert defs.get_assets_def(source_key("industry_nace_mappings")) is not None
    assert defs.get_assets_def(source_key("company_explorer_cache")) is not None


def test_finland_prhytj_assets_are_grouped_by_source_and_tagged_by_country():
    from dagster_corpscout.definitions import defs

    graph = defs.resolve_asset_graph()
    expected_layers = {
        "source_system": "external",
        "raw_snapshot": "raw",
        "normalized_tables": "normalized",
        "code_lists": "reference",
        "industry_nace_mappings": "mapping",
        "company_explorer_cache": "serving",
    }

    for asset_name, layer in expected_layers.items():
        node = graph.get(source_key(asset_name))

        assert node.group_name == "source_finland_prh_ytj"
        assert node.tags["country"] == "finland"
        assert node.tags["source"] == "prh_ytj"
        assert node.tags["source_name"] == "finland_prhytj"
        assert node.tags["layer"] == layer


def test_definitions_are_registry_driven():
    from dagster_corpscout.registry import all_assets, all_asset_checks, all_jobs, all_schedules
    from dagster_corpscout.source_bundle import SourceBundle
    from dagster_corpscout.sources.finland.prh_ytj import spec
    from dagster_corpscout.sources.finland.prh_ytj import source_bundle

    assert isinstance(source_bundle, SourceBundle)
    assert source_bundle.source_name == spec.SOURCE_NAME
    assert source_bundle.asset_key_prefix == tuple(spec.ASSET_KEY_PREFIX)
    assert source_bundle.assets
    assert all(asset in all_assets for asset in source_bundle.assets)
    assert all(check in all_asset_checks for check in source_bundle.asset_checks)
    assert all(job in all_jobs for job in source_bundle.jobs)
    assert all(schedule in all_schedules for schedule in source_bundle.schedules)


def test_pull_schedule_exists_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.resolve_schedule_def("finland_prhytj_pull_schedule")
    assert schedule.cron_schedule == "0 3 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "finland_prhytj_pipeline"


def test_transform_latest_job_excludes_raw_download():
    from dagster_corpscout.definitions import defs
    from dagster_corpscout.sources.finland.prh_ytj.jobs import transform_latest_job

    defs.resolve_job_def("finland_prhytj_transform_latest")
    selected = set(transform_latest_job.selection.selected_keys)

    assert source_key("raw_snapshot") not in selected
    assert selected == {
        source_key("normalized_tables"),
        source_key("code_lists"),
        source_key("industry_nace_mappings"),
        source_key("company_explorer_cache"),
    }
