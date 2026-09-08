"""What is left of the Sweden address-geocoding chain, as a shape guard.

Moved out of tests/test_sweden_company_address_geocoding.py when slice 4c deleted that file
with the chain it tested. The weekly is the only geocoding job, and this is the list of
assets it may select -- a re-added canonical/shared/demand/resolution step fails here first.
"""

import dagster as dg


def test_the_weekly_is_the_only_geocoding_job_and_selects_five_assets() -> None:
    """Slice 4b: the canonical/shared/demand/resolution/store chain is gone, and with it
    every job that selected it. What is left is the weekly, which refreshes the OSM extract,
    republishes the centroids, warms the address entity's geocode cache and forces the
    serving view's refresh -- the five steps the entity actually needs."""
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    weekly_job = repo.get_job("sweden_company_address_geocoding_weekly_job")
    schedule = repo.get_schedule_def("sweden_company_address_geocoding_weekly")

    assert {key.path[-1] for key in weekly_job.asset_layer.executable_asset_keys} == {
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
        "sweden_companies_current_clickhouse",
    }
    assert schedule.job.name == "sweden_company_address_geocoding_weekly_job"
    assert schedule.cron_schedule == "5 4 * * 2"
    assert schedule.execution_timezone == "Europe/Stockholm"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING

    graph_keys = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}
    job_names = {job.name for job in repo.get_all_jobs()}
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
        assert retired not in graph_keys, retired
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
