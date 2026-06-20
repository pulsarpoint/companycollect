from dagster import AssetKey

from dagster_v3.definitions import defs as load_project_defs


def test_gleif_assets_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_names = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "gleif_full_raw_reference_files" in asset_names
    assert "gleif_delta_raw_reference_files" in asset_names
    assert "gleif_reference_duckdb_state" in asset_names
    assert "gleif_reference_clickhouse" in asset_names
    assert "gleif_raw_retention" in asset_names


def test_gleif_raw_assets_have_no_upstream_dependencies() -> None:
    repository = load_project_defs().get_repository_def()
    graph = repository.asset_graph

    assert graph.get(AssetKey(["gleif_full_raw_reference_files"])).parent_keys == set()
    assert graph.get(AssetKey(["gleif_delta_raw_reference_files"])).parent_keys == set()


def test_gleif_duckdb_state_depends_on_both_raw_modes() -> None:
    repository = load_project_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_reference_duckdb_state"]))

    assert asset.parent_keys == {
        AssetKey(["gleif_full_raw_reference_files"]),
        AssetKey(["gleif_delta_raw_reference_files"]),
    }


def test_gleif_clickhouse_asset_depends_on_duckdb_state() -> None:
    repository = load_project_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_reference_clickhouse"]))

    assert asset.parent_keys == {AssetKey(["gleif_reference_duckdb_state"])}


def test_gleif_retention_asset_depends_on_clickhouse_export() -> None:
    repository = load_project_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_raw_retention"]))

    assert asset.parent_keys == {AssetKey(["gleif_reference_clickhouse"])}


def test_gleif_jobs_and_delta_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    schedule = next(
        item for item in repository.schedule_defs if item.name == "gleif_reference_delta_daily"
    )

    assert "gleif_reference_bootstrap_job" in set(repository.job_names)
    assert "gleif_reference_delta_job" in set(repository.job_names)
    assert schedule.job_name == "gleif_reference_delta_job"
    assert schedule.cron_schedule == "30 20 * * *"
    assert schedule.execution_timezone == "UTC"


def test_gleif_clickhouse_export_uses_catalog_qualified_duckdb_schema() -> None:
    from dagster_v3.defs.gleif import assets

    assert assets.GLEIF_DUCKDB_PATH.name == "gleif.duckdb"
    assert assets.GLEIF_DUCKDB_SCHEMA == "gleif.gleif"
