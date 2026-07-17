"""Graph-contract tests for the Norway Brreg definitions.

Coverage:
1. All surviving assets are registered, including norway_brreg_translation_load.
2. Dependency edges: parquet entity assets feed parquet-backed ClickHouse publish assets.
3. norway_brreg_entities_full_snapshot_job uses the full snapshot parquet-to-ClickHouse path.
4. norway_brreg_entity_updates_job uses the daily update parquet-to-ClickHouse path.
5. legacy Norway DuckDB/dbt assets and resources are no longer registered.
"""

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
import dagster_v3.defs.norway_brreg_financial.assets as brreg_financial_assets
from dagster_v3.defs.norway_brreg.entity_storage import (
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource
from dagster_v3.definitions import defs as load_project_defs


def test_norway_brreg_all_assets_registered() -> None:
    """The Definitions object includes all surviving assets and excludes removed ones."""
    repo = load_project_defs().get_repository_def()
    asset_names = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}

    assert "norway_brreg_entries_snapshot_raw_s3" in asset_names
    assert "norway_brreg_entries_snapshot_csv_raw_s3" in asset_names
    assert "norway_brreg_entities_snapshot_s3" in asset_names
    assert "norway_brreg_entity_updates_s3" in asset_names
    assert "norway_brreg_entities_snapshot_no_companies_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_no_company_contacts_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_no_company_addresses_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_no_websites_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_no_industries_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_affected_orgs_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_removed_orgs_parquet" in asset_names
    assert "norway_brreg_entity_updates_no_companies_parquet" in asset_names
    assert "norway_brreg_entity_updates_no_company_contacts_parquet" in asset_names
    assert "norway_brreg_entity_updates_no_company_addresses_parquet" in asset_names
    assert "norway_brreg_entity_updates_no_websites_parquet" in asset_names
    assert "norway_brreg_entity_updates_no_industries_parquet" in asset_names
    assert "norway_brreg_entity_updates_affected_orgs_parquet" in asset_names
    assert "norway_brreg_entity_updates_removed_orgs_parquet" in asset_names
    assert "norway_brreg_entities_snapshot_clickhouse" in asset_names
    assert "norway_brreg_entity_updates_clickhouse" in asset_names
    assert "norway_brreg_entities_snapshot_normalized_parquet" not in asset_names
    assert "norway_brreg_entity_updates_normalized_parquet" not in asset_names
    assert "norway_brreg_entities_duckdb" not in asset_names
    assert "norway_brreg_financial_fetches_duckdb" not in asset_names
    assert "norway_brreg_financial_statements_duckdb" not in asset_names
    assert "norway_resolved_no_companies" not in asset_names
    assert "norway_resolved_no_websites" not in asset_names
    assert "norway_resolved_no_industries" not in asset_names
    assert "norway_resolved_no_financial_statements" not in asset_names
    assert "norway_resolved_clickhouse" not in asset_names
    assert "norway_brreg_financial_bootstrap_responses_json" in asset_names
    assert "norway_brreg_financial_bootstrap_responses_parquet" in asset_names
    assert "norway_brreg_financial_responses_updates_json" in asset_names
    assert "norway_brreg_financial_responses_updates_parquet" in asset_names
    assert "norway_brreg_financial_bootstrap_fetches_parquet" not in asset_names
    assert "norway_brreg_financial_fetches_updates_parquet" not in asset_names
    assert "norway_brreg_financial_fetches_snapshot_parquet" not in asset_names
    assert "norway_brreg_financial_statements_snapshot_parquet" in asset_names
    assert "norway_brreg_financial_statements_updates_parquet" in asset_names
    assert "norway_brreg_financial_statements_snapshot_usd_parquet" in asset_names
    assert "norway_brreg_financial_statements_updates_usd_parquet" in asset_names
    assert "norway_brreg_financial_statements_snapshot_clickhouse" in asset_names
    assert "norway_brreg_financial_statements_updates_clickhouse" in asset_names
    assert "norway_brreg_translation_load" in asset_names
    assert "norway_brreg_translation_trigger" not in asset_names
    assert "norway_brreg_clickhouse_canonical_contacts" in asset_names

    # Raw Brreg ClickHouse exports and old translation-in-graph assets are gone.
    assert "norway_brreg_clickhouse_companies" not in asset_names
    assert "norway_brreg_clickhouse_financial_statements" not in asset_names
    assert "norway_brreg_translation_queue" not in asset_names
    assert "norway_brreg_translation_workflow_status" not in asset_names


def test_norway_brreg_asset_dependency_edges() -> None:
    """Dep-edges in the graph after raw ClickHouse export assets were removed (Task 4)."""
    asset_graph = load_project_defs().get_repository_def().asset_graph

    snapshot_clickhouse_node = asset_graph.get(
        dg.AssetKey("norway_brreg_entities_snapshot_clickhouse")
    )
    update_clickhouse_node = asset_graph.get(
        dg.AssetKey("norway_brreg_entity_updates_clickhouse")
    )
    loader_node = asset_graph.get(dg.AssetKey("norway_brreg_translation_load"))
    canonical_contacts_node = asset_graph.get(
        dg.AssetKey("norway_brreg_clickhouse_canonical_contacts")
    )
    snapshot_raw_node = asset_graph.get(
        dg.AssetKey("norway_brreg_entries_snapshot_raw_s3")
    )
    snapshot_csv_raw_node = asset_graph.get(
        dg.AssetKey("norway_brreg_entries_snapshot_csv_raw_s3")
    )
    snapshot_contacts_node = asset_graph.get(
        dg.AssetKey("norway_brreg_entities_snapshot_no_company_contacts_parquet")
    )
    snapshot_addresses_node = asset_graph.get(
        dg.AssetKey("norway_brreg_entities_snapshot_no_company_addresses_parquet")
    )
    snapshot_s3_node = asset_graph.get(dg.AssetKey("norway_brreg_entities_snapshot_s3"))
    bootstrap_json_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_bootstrap_responses_json.key
    )
    bootstrap_parquet_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_bootstrap_responses_parquet.key
    )
    update_json_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_responses_updates_json.key
    )
    update_parquet_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_responses_updates_parquet.key
    )
    snapshot_statements_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_statements_snapshot_parquet.key
    )
    update_statements_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_statements_updates_parquet.key
    )
    snapshot_usd_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_statements_snapshot_usd_parquet.key
    )
    update_usd_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_statements_updates_usd_parquet.key
    )
    snapshot_financial_clickhouse_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_statements_snapshot_clickhouse.key
    )
    update_financial_clickhouse_node = asset_graph.get(
        brreg_financial_assets.norway_brreg_financial_statements_updates_clickhouse.key
    )

    assert {k.path[-1] for k in snapshot_clickhouse_node.parent_keys} == {
        "norway_brreg_entities_snapshot_no_companies_parquet",
        "norway_brreg_entities_snapshot_no_company_contacts_parquet",
        "norway_brreg_entities_snapshot_no_company_addresses_parquet",
        "norway_brreg_entities_snapshot_no_websites_parquet",
        "norway_brreg_entities_snapshot_no_industries_parquet",
    }
    assert {k.path[-1] for k in update_clickhouse_node.parent_keys} == {
        "norway_brreg_entity_updates_no_companies_parquet",
        "norway_brreg_entity_updates_no_company_contacts_parquet",
        "norway_brreg_entity_updates_no_company_addresses_parquet",
        "norway_brreg_entity_updates_no_websites_parquet",
        "norway_brreg_entity_updates_no_industries_parquet",
        "norway_brreg_entity_updates_affected_orgs_parquet",
        "norway_brreg_entity_updates_removed_orgs_parquet",
    }
    # Both ClickHouse landing paths (snapshot publish AND daily updates) feed
    # the translation loader, so newly ingested texts are enqueued whichever
    # path landed them.
    assert {k.path[-1] for k in loader_node.parent_keys} == {
        "norway_brreg_entities_snapshot_clickhouse",
        "norway_brreg_entity_updates_clickhouse",
    }
    # Same reasoning as the translation loader: the canonical-contacts derivation
    # reshapes corpscout.no_websites, which either ClickHouse landing path writes.
    assert {k.path[-1] for k in canonical_contacts_node.parent_keys} == {
        "norway_brreg_entities_snapshot_clickhouse",
        "norway_brreg_entity_updates_clickhouse",
    }
    assert {k.path[-1] for k in snapshot_raw_node.parent_keys} == set()
    assert {k.path[-1] for k in snapshot_csv_raw_node.parent_keys} == set()
    assert {k.path[-1] for k in snapshot_contacts_node.parent_keys} == {
        "norway_brreg_entries_snapshot_csv_raw_s3"
    }
    assert {k.path[-1] for k in snapshot_addresses_node.parent_keys} == {
        "norway_brreg_entries_snapshot_csv_raw_s3"
    }
    assert {k.path[-1] for k in snapshot_s3_node.parent_keys} == {
        "norway_brreg_entries_snapshot_raw_s3"
    }
    assert {k.path[-1] for k in bootstrap_json_node.parent_keys} == set()
    assert {k.path[-1] for k in bootstrap_parquet_node.parent_keys} == {
        "norway_brreg_financial_bootstrap_responses_json"
    }
    assert {k.path[-1] for k in update_json_node.parent_keys} == set()
    assert {k.path[-1] for k in update_parquet_node.parent_keys} == {
        "norway_brreg_financial_responses_updates_json"
    }
    assert {k.path[-1] for k in snapshot_statements_node.parent_keys} == {
        "norway_brreg_financial_bootstrap_responses_parquet"
    }
    assert {k.path[-1] for k in update_statements_node.parent_keys} == {
        "norway_brreg_financial_responses_updates_parquet"
    }
    assert {k.path[-1] for k in snapshot_usd_node.parent_keys} == {
        "norway_brreg_financial_statements_snapshot_parquet"
    }
    assert {k.path[-1] for k in update_usd_node.parent_keys} == {
        "norway_brreg_financial_statements_updates_parquet"
    }
    assert {k.path[-1] for k in snapshot_financial_clickhouse_node.parent_keys} == {
        "norway_brreg_financial_statements_snapshot_usd_parquet",
        "norway_brreg_entities_snapshot_clickhouse",
    }
    assert {k.path[-1] for k in update_financial_clickhouse_node.parent_keys} == {
        "norway_brreg_financial_statements_updates_usd_parquet",
        "norway_brreg_entity_updates_affected_orgs_parquet",
        "norway_brreg_entity_updates_clickhouse",
    }


def test_norway_brreg_entities_full_snapshot_job_membership() -> None:
    """norway_brreg_entities_full_snapshot_job contains the full expanded upstream selection."""
    repo = load_project_defs().get_repository_def()
    assert "norway_brreg_entities_full_snapshot_job" in repo.job_names
    assert "norway_brreg_refresh_job" not in repo.job_names
    assert "norway_brreg_entity_updates_job" in repo.job_names
    assert "norway_brreg_translation_completion_job" not in repo.job_names

    refresh = {
        k.path[-1]
        for k in repo.get_job(
            "norway_brreg_entities_full_snapshot_job"
        ).asset_layer.executable_asset_keys
    }
    # The translation loader and canonical-contacts derivation run after the
    # parquet-backed full snapshot publish.
    assert refresh == {
        "norway_brreg_entries_snapshot_raw_s3",
        "norway_brreg_entries_snapshot_csv_raw_s3",
        "norway_brreg_entities_snapshot_s3",
        "norway_brreg_entities_snapshot_no_companies_parquet",
        "norway_brreg_entities_snapshot_no_company_contacts_parquet",
        "norway_brreg_entities_snapshot_no_company_addresses_parquet",
        "norway_brreg_entities_snapshot_no_websites_parquet",
        "norway_brreg_entities_snapshot_no_industries_parquet",
        "norway_brreg_entities_snapshot_affected_orgs_parquet",
        "norway_brreg_entities_snapshot_removed_orgs_parquet",
        "norway_brreg_entities_snapshot_clickhouse",
        "norway_brreg_translation_load",
        "norway_brreg_clickhouse_canonical_contacts",
    }
    assert "norway_resolved_clickhouse" not in refresh
    assert "norway_resolved_no_companies" not in refresh
    # Raw brreg ClickHouse exports are no longer in the job chain
    assert "norway_brreg_clickhouse_companies" not in refresh
    assert "norway_brreg_clickhouse_financial_statements" not in refresh
    # Old translation-in-graph assets must be absent from the job
    assert "norway_brreg_translation_queue" not in refresh
    assert "norway_brreg_translations_applied" not in refresh


def test_norway_brreg_entity_updates_job_membership() -> None:
    """norway_brreg_entity_updates_job contains the daily parquet update chain."""
    repo = load_project_defs().get_repository_def()

    update = {
        k.path[-1]
        for k in repo.get_job(
            "norway_brreg_entity_updates_job"
        ).asset_layer.executable_asset_keys
    }

    assert update == {
        "norway_brreg_entity_updates_s3",
        "norway_brreg_entity_updates_no_companies_parquet",
        "norway_brreg_entity_updates_no_company_contacts_parquet",
        "norway_brreg_entity_updates_no_company_addresses_parquet",
        "norway_brreg_entity_updates_no_websites_parquet",
        "norway_brreg_entity_updates_no_industries_parquet",
        "norway_brreg_entity_updates_affected_orgs_parquet",
        "norway_brreg_entity_updates_removed_orgs_parquet",
        "norway_brreg_entity_updates_clickhouse",
        # The translation loader runs at the end of every daily update run so
        # newly landed texts are enqueued to the translator service.
        "norway_brreg_translation_load",
        # The canonical-contacts derivation reshapes corpscout.no_websites at the
        # end of every daily update run too.
        "norway_brreg_clickhouse_canonical_contacts",
    }


def test_norway_brreg_financial_asset_jobs_are_removed() -> None:
    repo = load_project_defs().get_repository_def()
    assert "norway_brreg_financial_snapshot_job" not in repo.job_names
    assert "norway_brreg_financial_updates_job" not in repo.job_names


def test_norway_brreg_resources_are_wired() -> None:
    """Norway Brreg uses API and parquet storage resources, not the old DuckDB resource."""
    top_level_resources = (
        load_project_defs().get_repository_def().get_top_level_resources()
    )

    assert "norway_brreg_duckdb" not in top_level_resources
    assert "norway_brreg_api" in top_level_resources
    assert (
        top_level_resources["norway_brreg_api"].configurable_resource_cls
        is NorwayBrregApiResource
    )
    assert "object_store" in top_level_resources
    assert (
        top_level_resources["object_store"].configurable_resource_cls
        is ObjectStoreResource
    )
    assert "norway_brreg_entity_storage" in top_level_resources
    assert (
        top_level_resources["norway_brreg_entity_storage"].configurable_resource_cls
        is NorwayBrregEntityParquetStorageResource
    )
    assert "norway_brreg_financial_storage" in top_level_resources
    assert (
        top_level_resources["norway_brreg_financial_storage"].configurable_resource_cls
        is NorwayBrregFinancialParquetStorageResource
    )
    assert "norway_brreg_translation_queue_duckdb" not in top_level_resources


def test_norway_brreg_entities_full_snapshot_has_no_schedule() -> None:
    """The full snapshot job is manual-only; daily updates own the recurring schedule."""
    repo = load_project_defs().get_repository_def()
    schedule_names = {schedule.name for schedule in repo.schedule_defs}

    assert "norway_brreg_entities_full_snapshot_schedule" not in schedule_names
    assert "norway_brreg_refresh_schedule" not in schedule_names


def test_norway_brreg_entity_updates_schedule_wiring() -> None:
    """norway_brreg_entity_updates_schedule is daily and wired to the partitioned update job."""
    repo = load_project_defs().get_repository_def()
    sched = repo.get_schedule_def("norway_brreg_entity_updates_schedule")

    assert sched.cron_schedule == "0 0 * * *"
    assert sched.job.name == "norway_brreg_entity_updates_job"
