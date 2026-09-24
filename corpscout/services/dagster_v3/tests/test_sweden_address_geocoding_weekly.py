"""The Sweden weekly address chain, as a shape guard.

Spec: docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md. The weekly is the one
job that publishes Sweden addresses on a schedule: extract, normalize, refresh OSM, centroids,
warm the geocode cache, fold, then refresh the companies serving view. This file pins the
selection, the order-bearing dependency, the schedule, and that every DuckDB-touching step is
in the single-slot pool. It builds module-scoped Definitions rather than loading the whole
project: the full load needs env vars pytest does not carry (see `dg check defs` instead).
"""

import dagster as dg

from dagster_v3.defs.se_company.address import assets as address_assets
from dagster_v3.defs.se_company.address import bolagsverket as address_bolagsverket
from dagster_v3.defs.se_company.address import esef as address_esef
from dagster_v3.defs.se_company.address import ratsit as address_ratsit
from dagster_v3.defs.se_company.address import scb as address_scb
from dagster_v3.defs.sweden_address_osm import assets as osm_assets
from dagster_v3.defs.sweden_company import (
    address_geocoding_assets as weekly,
    centroid_assets,
    companies_current_asset,
)

WEEKLY_JOB = "sweden_company_address_geocoding_weekly_job"
WEEKLY_SCHEDULE = "sweden_company_address_geocoding_weekly"
DUCKDB_POOL = "sweden_address_osm_duckdb"

EXPECTED_SELECTION = {
    "se_company_address_suggestions_scb",
    "se_company_address_suggestions_bolagsverket",
    "se_company_address_suggestions_ratsit",
    "se_company_address_suggestions_esef",
    "se_company_address_normalize",
    "sweden_osm_pbf_s3",
    "sweden_osm_addresses_duckdb",
    "sweden_geocode_centroids_clickhouse",
    "se_address_geocodes_warm",
    "se_company_address_publish",
    "sweden_companies_current_clickhouse",
}


def _weekly_defs() -> dg.Definitions:
    """Only the modules the weekly touches, with mock resources so the job resolves.

    The four extractor assets (EXTRACTOR_ASSET_NAMES) are defined per source in
    bolagsverket.py/esef.py/ratsit.py/scb.py, not in assets.py -- those modules must be
    loaded too or resolve_job_def raises on the missing keys.
    """
    assets = dg.load_assets_from_modules(
        [
            address_assets,
            address_bolagsverket,
            address_esef,
            address_ratsit,
            address_scb,
            osm_assets,
            centroid_assets,
            companies_current_asset,
            weekly,
        ]
    )
    return dg.Definitions(
        assets=assets,
        jobs=[weekly.sweden_company_address_geocoding_weekly_job],
        schedules=[weekly.sweden_company_address_geocoding_weekly],
        resources={
            "clickhouse": dg.ResourceDefinition.mock_resource(),
            "sweden_address_osm_duckdb": dg.ResourceDefinition.mock_resource(),
            "sweden_address_osm_object_store": dg.ResourceDefinition.mock_resource(),
        },
    )


def test_the_weekly_selects_the_eleven_step_chain() -> None:
    job = _weekly_defs().resolve_job_def(WEEKLY_JOB)
    assert {key.path[-1] for key in job.asset_layer.executable_asset_keys} == EXPECTED_SELECTION
    assert len(EXPECTED_SELECTION) == 11


def test_the_weekly_runs_tuesday_0105_stockholm() -> None:
    schedule = _weekly_defs().resolve_schedule_def(WEEKLY_SCHEDULE)
    assert schedule.job.name == WEEKLY_JOB
    assert schedule.cron_schedule == "5 1 * * 2"
    assert weekly.WEEKLY_CRON_SCHEDULE == "5 1 * * 2"
    assert schedule.execution_timezone == "Europe/Stockholm"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_every_duckdb_touching_step_is_in_the_single_slot_pool() -> None:
    """The 2026-08-25 lock conflict came from two runs opening the OSM DuckDB file outside a
    pool. Every step of the chain that requires the DuckDB resource must serialize on it."""
    job = _weekly_defs().resolve_job_def(WEEKLY_JOB)
    duckdb_steps = {
        node.name: node.pool
        for node in job.graph.node_defs
        if DUCKDB_POOL in node.required_resource_keys
    }
    assert duckdb_steps, "the chain must contain DuckDB-touching steps"
    assert all(pool == DUCKDB_POOL for pool in duckdb_steps.values()), duckdb_steps
    assert set(duckdb_steps) >= {
        "sweden_osm_addresses_duckdb",
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
        "se_company_address_publish",
    }


def test_the_extractors_run_for_real_not_in_preview() -> None:
    """ExtractConfig.execute defaults to False (preview: count, write nothing). The weekly must
    carry execute=True for all four extractor ops or the chain publishes stale sources forever."""
    job = _weekly_defs().resolve_job_def(WEEKLY_JOB)
    ops = job.run_config["ops"]
    for name in address_assets.EXTRACTOR_ASSET_NAMES:
        assert ops[name]["config"]["execute"] is True, name
        assert ops[name]["config"]["page_size"] == weekly.EXTRACTOR_PAGE_SIZE, name
    assert set(ops) == set(address_assets.EXTRACTOR_ASSET_NAMES)


def test_the_fold_waits_for_normalize_warm_and_centroids() -> None:
    assert {key.path[-1] for key in address_assets.se_company_address_publish.dependency_keys} == {
        "se_company_address_normalize",
        "se_address_geocodes_warm",
        "sweden_geocode_centroids_clickhouse",
    }


def test_the_retired_geocoding_chain_stays_retired() -> None:
    """Slice 4b deleted the canonical/shared/demand/resolution/store chain. None of it may
    come back into the sweden_company modules under its old names. The weekly module itself is
    loaded here (via _weekly_defs) and pinned to exactly one job and one schedule."""
    defs = _weekly_defs()
    asset_names = {key.path[-1] for key in defs.resolve_asset_graph().get_all_asset_keys()}
    job_names = {job.name for job in defs.resolve_all_job_defs()}
    weekly_job_names = {job.name for job in weekly.defs.jobs}
    assert {job.name for job in weekly.defs.jobs} == {WEEKLY_JOB}
    assert {schedule.name for schedule in weekly.defs.schedules} == {WEEKLY_SCHEDULE}
    for retired in (
        "sweden_company_addresses_clickhouse",
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_address_geocode_demand_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_resolution_unmatched_diagnostics_duckdb",
        "sweden_address_geocode_store_clickhouse",
        "sweden_address_geocode_store_backfill_clickhouse",
        "sweden_address_geocode_legacy_adoption_clickhouse",
    ):
        assert retired not in asset_names, retired
    for retired_job in (
        "sweden_company_address_geocoding_job",
        "sweden_shared_address_identity_job",
        "sweden_shared_address_geocoding_job",
        "sweden_address_geocode_store_backfill_job",
        "sweden_address_geocode_legacy_adoption_job",
        "sweden_address_resolution_shadow_job",
        "sweden_address_resolution_publish_job",
        "sweden_address_resolution_diagnostics_job",
    ):
        assert retired_job not in job_names, retired_job
        assert retired_job not in weekly_job_names, retired_job
