"""What is left of the Sweden geocoding pipeline after slice 4b: the weekly job.

The canonical/shared/demand/resolution/store chain this module used to hold retired with
the old address model. The SE address model is the address entity
(`corpscout.se_company_address`, built by `se_company/address`), which geocodes inside its
own warm step and fold, so nothing here computes addresses or coordinates any more.

This module now only assembles the weekly run out of assets that live elsewhere: the OSM
snapshot and index (`sweden_address_osm`), the coarse centroids (`centroid_assets.py`), the
address entity's geocode-cache warm (`se_company/address/assets.py`) and the companies
serving-view refresh (`companies_current_asset.py`).
"""

import dagster as dg

from dagster_v3.defs.sweden_company.centroid_assets import (
    CENTROIDS_ASSET_KEY,
    sweden_geocode_centroid_area_sanity_check,
    sweden_geocode_centroids_clickhouse,
)
from dagster_v3.defs.sweden_company.companies_current_asset import (
    COMPANIES_CURRENT_ASSET_KEY,
    sweden_companies_current_clickhouse,
    sweden_companies_current_refresh_check,
)

GROUP_NAME = "sweden_company"
WEEKLY_CRON_SCHEDULE = "5 4 * * 2"
WEEKLY_EXECUTION_TIMEZONE = "Europe/Stockholm"


sweden_company_address_geocoding_weekly_job = dg.define_asset_job(
    name="sweden_company_address_geocoding_weekly_job",
    selection=dg.AssetSelection.assets(
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        CENTROIDS_ASSET_KEY,
        "se_address_geocodes_warm",
        COMPANIES_CURRENT_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "address_geocoding"},
    description=(
        "Refreshes the Sweden Geofabrik snapshot and OSM address index, republishes the "
        "postcode and city centroids, warms the address entity's geocode cache against the "
        "new extract, and forces the companies serving view to refresh."
    ),
)

sweden_company_address_geocoding_weekly = dg.ScheduleDefinition(
    name="sweden_company_address_geocoding_weekly",
    job=sweden_company_address_geocoding_weekly_job,
    cron_schedule=WEEKLY_CRON_SCHEDULE,
    execution_timezone=WEEKLY_EXECUTION_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description=(
        "Weekly Sweden OSM snapshot and address index, then the centroids and the address "
        "entity's geocode cache are rebuilt against the new extract and the companies "
        "serving view is refreshed."
    ),
)


defs = dg.Definitions(
    assets=[
        sweden_geocode_centroids_clickhouse,
        sweden_companies_current_clickhouse,
    ],
    asset_checks=[
        sweden_geocode_centroid_area_sanity_check,
        sweden_companies_current_refresh_check,
    ],
    jobs=[sweden_company_address_geocoding_weekly_job],
    schedules=[sweden_company_address_geocoding_weekly],
)
