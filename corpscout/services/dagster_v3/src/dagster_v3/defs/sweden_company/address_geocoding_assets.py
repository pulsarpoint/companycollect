"""The Sweden weekly address chain: extract, normalize, OSM, centroids, warm, fold, serving refresh.

The canonical/shared/demand/resolution/store chain this module used to hold retired with the
old address model (slice 4b). The SE address model is the address entity
(`corpscout.se_company_address`, built by `se_company/address`), which geocodes inside its
own warm step and fold, so nothing here computes addresses or coordinates.

This module assembles the weekly run out of assets that live elsewhere, in dependency order:
the four source extractors and the normalizer (`se_company/address/assets.py`), the OSM
snapshot and index (`sweden_address_osm`), the coarse centroids (`centroid_assets.py`), the
address entity's geocode-cache warm and the fold that publishes every company's addresses
(`se_company/address/assets.py`), and the companies serving-view refresh
(`companies_current_asset.py`), which depends on the fold so it runs last.

Geocoding precedes the fold on purpose: the fold writes coordinates onto the published rows by
reading the geocode cache, so a fold before warming would match page by page (4 h 18 m on
2026-09-07) and a warm after the fold would reach users only a week later. The fold runs with
`AddressFoldConfig` defaults (`changed_only=True`); until the cache-invalidation redesign
lands, the weekly OSM extract still marks every company stale, so the fold rewrites all 64
buckets (~75 min with the warm cache; measured 2026-09-24). The run starts Tuesday 01:05
Europe/Stockholm so it holds the single-slot DuckDB pool (shared with the backoffice's Fold
now) well before reviewers start. The job's default run config sets execute=True and
page_size=10,000 on the four extractor ops; without it they run in preview and write nothing.
Spec: docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md.
"""

import dagster as dg

from dagster_v3.defs.se_company.address.assets import EXTRACTOR_ASSET_NAMES
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

WEEKLY_CRON_SCHEDULE = "5 1 * * 2"
WEEKLY_EXECUTION_TIMEZONE = "Europe/Stockholm"
# The extractors default to preview (ExtractConfig.execute=False writes nothing). The weekly must
# write, with the same page size the backoffice's Processing page sends them.
EXTRACTOR_PAGE_SIZE = 10_000


sweden_company_address_geocoding_weekly_job = dg.define_asset_job(
    name="sweden_company_address_geocoding_weekly_job",
    selection=dg.AssetSelection.assets(
        *EXTRACTOR_ASSET_NAMES,
        "se_company_address_normalize",
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        CENTROIDS_ASSET_KEY,
        "se_address_geocodes_warm",
        "se_company_address_publish",
        COMPANIES_CURRENT_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "address_geocoding"},
    description=(
        "Weekly Sweden address chain: sync the source address extractors and normalize, "
        "refresh the Geofabrik snapshot and OSM address index, republish the centroids, warm "
        "the address entity's geocode cache against the new extract, fold and publish every "
        "company's addresses, then force the companies serving view to refresh."
    ),
    config={
        "ops": {
            name: {"config": {"execute": True, "page_size": EXTRACTOR_PAGE_SIZE}}
            for name in EXTRACTOR_ASSET_NAMES
        }
    },
)

sweden_company_address_geocoding_weekly = dg.ScheduleDefinition(
    name="sweden_company_address_geocoding_weekly",
    job=sweden_company_address_geocoding_weekly_job,
    cron_schedule=WEEKLY_CRON_SCHEDULE,
    execution_timezone=WEEKLY_EXECUTION_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description=(
        "Tuesday 01:05 Stockholm: extract, normalize, OSM refresh, centroids, geocode warm, "
        "fold of all company addresses, then the companies serving view refresh. About 2.5 h "
        "end to end while the fold rewrites every bucket; holds the sweden_address_osm_duckdb pool."
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
