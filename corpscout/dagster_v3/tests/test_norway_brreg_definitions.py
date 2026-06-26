"""Graph-contract tests for the Norway Brreg definitions after retargeting the translation trigger.

Coverage:
1. All surviving assets are registered, including norway_brreg_translation_trigger.
2. Dependency edges: entities→fetches, fetches→statements, entities→clickhouse_companies,
   statements→clickhouse_financial, norway_resolved_clickhouse→translation_trigger.
3. norway_brreg_refresh_job expanded membership now includes the resolved dbt models and
   resolved ClickHouse export via upstream() resolution across modules.
4. resources["norway_brreg_duckdb"] is wired as DuckDBResource.
"""

import dagster as dg
from dagster_duckdb import DuckDBResource

import dagster_v3.defs.norway_brreg.assets as brreg_assets
from dagster_v3.definitions import defs as load_project_defs


def test_norway_brreg_all_assets_registered() -> None:
    """The Definitions object includes all surviving assets and excludes removed ones."""
    repo = load_project_defs().get_repository_def()
    asset_names = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}

    assert "norway_brreg_entities_duckdb" in asset_names
    assert "norway_brreg_financial_fetches_duckdb" in asset_names
    assert "norway_brreg_financial_statements_duckdb" in asset_names
    assert "norway_brreg_translation_trigger" in asset_names

    # Raw ClickHouse export assets dropped in Task 4 (raw tables orphaned; replaced by
    # norway_resolved → no_companies / no_financial_statements pipeline).
    assert "norway_brreg_clickhouse_companies" not in asset_names
    assert "norway_brreg_clickhouse_financial_statements" not in asset_names
    # In-graph translation assets removed in d9d17e3 must be absent.
    # Note: norway_brreg_translations_applied still appears as an *external source* in
    # norway_resolved/dbt/models/sources.yml, so we only assert the pure Dagster assets are gone.
    assert "norway_brreg_translation_queue" not in asset_names
    assert "norway_brreg_translation_workflow_status" not in asset_names


def test_norway_brreg_asset_dependency_edges() -> None:
    """Dep-edges in the graph after raw ClickHouse export assets were removed (Task 4)."""
    asset_graph = load_project_defs().get_repository_def().asset_graph

    fetches_node = asset_graph.get(brreg_assets.norway_brreg_financial_fetches_duckdb_asset.key)
    statements_node = asset_graph.get(
        brreg_assets.norway_brreg_financial_statements_duckdb_asset.key
    )
    trigger_node = asset_graph.get(dg.AssetKey("norway_brreg_translation_trigger"))

    # entities → fetches
    assert {k.path[-1] for k in fetches_node.parent_keys} == {"norway_brreg_entities_duckdb"}
    # fetches → statements
    assert {k.path[-1] for k in statements_node.parent_keys} == {
        "norway_brreg_financial_fetches_duckdb"
    }
    # norway_resolved_clickhouse → translation_trigger  (fires after no_companies lands)
    assert {k.path[-1] for k in trigger_node.parent_keys} == {
        "norway_resolved_clickhouse"
    }


def test_norway_brreg_refresh_job_membership() -> None:
    """norway_brreg_refresh_job contains the full expanded upstream selection."""
    repo = load_project_defs().get_repository_def()
    assert "norway_brreg_refresh_job" in repo.job_names
    assert "norway_brreg_translation_completion_job" not in repo.job_names

    refresh = {
        k.path[-1]
        for k in repo.get_job("norway_brreg_refresh_job").asset_layer.executable_asset_keys
    }
    # Trigger fires after the resolved export; the full chain runs through it.
    assert refresh == {
        # brreg raw chain
        "norway_brreg_entities_duckdb",
        "norway_brreg_financial_fetches_duckdb",
        "norway_brreg_financial_statements_duckdb",
        # resolved dbt models
        "norway_resolved_no_companies",
        "norway_resolved_no_websites",
        "norway_resolved_no_industries",
        "norway_resolved_no_financial_statements",
        # resolved ClickHouse export + trigger
        "norway_resolved_clickhouse",
        "norway_brreg_translation_trigger",
    }
    # Raw brreg ClickHouse exports are no longer in the job chain
    assert "norway_brreg_clickhouse_companies" not in refresh
    assert "norway_brreg_clickhouse_financial_statements" not in refresh
    # Old translation-in-graph assets must be absent from the job
    assert "norway_brreg_translation_queue" not in refresh
    assert "norway_brreg_translations_applied" not in refresh


def test_norway_brreg_duckdb_resource_is_wired() -> None:
    """The norway_brreg_duckdb resource is a DuckDBResource; the removed queue resource is gone."""
    top_level_resources = load_project_defs().get_repository_def().get_top_level_resources()

    assert "norway_brreg_duckdb" in top_level_resources
    assert top_level_resources["norway_brreg_duckdb"].configurable_resource_cls is DuckDBResource
    assert "norway_brreg_translation_queue_duckdb" not in top_level_resources


def test_norway_brreg_duckdb_pool_on_writing_assets() -> None:
    """Every surviving DuckDB-writing asset declares the norway_brreg_duckdb pool."""
    assert brreg_assets.norway_brreg_entities_duckdb_asset.op.pool == "norway_brreg_duckdb"
    assert (
        brreg_assets.norway_brreg_financial_fetches_duckdb_asset.op.pool
        == "norway_brreg_duckdb"
    )
    assert (
        brreg_assets.norway_brreg_financial_statements_duckdb_asset.op.pool
        == "norway_brreg_duckdb"
    )


def test_norway_brreg_refresh_schedule_wiring() -> None:
    """norway_brreg_refresh_schedule is monthly, staggered, wired to the refresh job."""
    repo = load_project_defs().get_repository_def()
    sched = repo.get_schedule_def("norway_brreg_refresh_schedule")

    assert sched.cron_schedule == "0 6 7 * *"
    assert sched.job.name == "norway_brreg_refresh_job"
