import dagster as dg
import pytest
from dagster import AssetKey

from dagster_v3.defs.gleif import assets
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
)


def load_gleif_defs():
    return dg.Definitions.merge(
        assets.defs,
        dg.Definitions(
            resources={
                "object_store": dg.ResourceDefinition.hardcoded_resource(object()),
                "clickhouse": dg.ResourceDefinition.hardcoded_resource(object()),
                "dlt": dg.ResourceDefinition.hardcoded_resource(object()),
            }
        ),
    )


def test_gleif_assets_are_registered() -> None:
    repository = load_gleif_defs().get_repository_def()
    asset_names = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "gleif_full_raw_reference_files" in asset_names
    assert "gleif_delta_raw_reference_files" in asset_names
    assert "gleif_raw_lei_records_duckdb" in asset_names
    assert "gleif_raw_relationships_duckdb" in asset_names
    assert "gleif_raw_reporting_exceptions_duckdb" in asset_names
    assert "gleif_reference_duckdb_state" in asset_names
    assert "gleif_reference_clickhouse" in asset_names
    assert "gleif_raw_retention" in asset_names


def test_gleif_raw_assets_have_no_upstream_dependencies() -> None:
    repository = load_gleif_defs().get_repository_def()
    graph = repository.asset_graph

    assert graph.get(AssetKey(["gleif_full_raw_reference_files"])).parent_keys == set()
    assert graph.get(AssetKey(["gleif_delta_raw_reference_files"])).parent_keys == set()


def test_gleif_dlt_raw_assets_depend_on_both_raw_modes() -> None:
    repository = load_gleif_defs().get_repository_def()
    graph = repository.asset_graph
    expected_parents = {
        AssetKey(["gleif_full_raw_reference_files"]),
        AssetKey(["gleif_delta_raw_reference_files"]),
    }

    assert graph.get(AssetKey(["gleif_raw_lei_records_duckdb"])).parent_keys == expected_parents
    assert graph.get(AssetKey(["gleif_raw_relationships_duckdb"])).parent_keys == expected_parents
    assert (
        graph.get(AssetKey(["gleif_raw_reporting_exceptions_duckdb"])).parent_keys
        == expected_parents
    )


def test_gleif_duckdb_state_depends_on_dlt_raw_assets() -> None:
    repository = load_gleif_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_reference_duckdb_state"]))

    assert asset.parent_keys == {
        AssetKey(["gleif_raw_lei_records_duckdb"]),
        AssetKey(["gleif_raw_relationships_duckdb"]),
        AssetKey(["gleif_raw_reporting_exceptions_duckdb"]),
    }


def test_gleif_clickhouse_asset_depends_on_duckdb_state() -> None:
    repository = load_gleif_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_reference_clickhouse"]))

    assert asset.parent_keys == {AssetKey(["gleif_reference_duckdb_state"])}


def test_gleif_retention_asset_depends_on_clickhouse_export() -> None:
    repository = load_gleif_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_raw_retention"]))

    assert asset.parent_keys == {AssetKey(["gleif_reference_clickhouse"])}


def test_gleif_jobs_and_delta_schedule_are_registered() -> None:
    repository = load_gleif_defs().get_repository_def()
    schedule = next(
        item for item in repository.schedule_defs if item.name == "gleif_reference_delta_daily"
    )

    assert "gleif_reference_bootstrap_job" in set(repository.job_names)
    assert "gleif_reference_delta_job" in set(repository.job_names)
    assert schedule.job_name == "gleif_reference_delta_job"
    assert schedule.cron_schedule == "30 20 * * *"
    assert schedule.execution_timezone == "UTC"


def test_gleif_clickhouse_export_uses_catalog_qualified_duckdb_schema() -> None:
    assert assets.GLEIF_DUCKDB_PATH.name == "gleif_reference.duckdb"
    assert assets.GLEIF_DUCKDB_SCHEMA == "gleif_reference.gleif"


def test_full_raw_count_validation_rejects_single_chunk_load() -> None:
    with pytest.raises(ValueError, match="too few rows"):
        assets._validate_raw_row_counts(
            manifest={"load_mode": "full"},
            row_counts={
                GLEIF_RAW_LEI_RECORDS_TABLE: 5_000,
                GLEIF_RAW_RELATIONSHIPS_TABLE: 476_870,
            },
        )


def test_delta_raw_count_validation_allows_small_loads() -> None:
    assets._validate_raw_row_counts(
        manifest={"load_mode": "delta"},
        row_counts={
            GLEIF_RAW_LEI_RECORDS_TABLE: 10,
            GLEIF_RAW_RELATIONSHIPS_TABLE: 10,
        },
    )
