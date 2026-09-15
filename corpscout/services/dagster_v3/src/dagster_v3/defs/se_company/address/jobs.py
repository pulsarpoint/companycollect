"""Global address source sync and full processing, launched from the backoffice."""

import dagster as dg

from dagster_v3.defs.se_company.address.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_address_normalize"

se_company_address_sync_job = dg.define_asset_job(
    "se_company_address_sync_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET),
    description="Sync ingested source addresses and normalize them. No geocoding or publishing.",
)
se_company_address_refresh_job = dg.define_asset_job(
    "se_company_address_refresh_job",
    selection=dg.AssetSelection.assets(
        *EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET, "se_address_geocodes_warm", "se_company_address_publish",
    ),
    description="Sync and normalize address inputs, warm geocodes from existing OSM data, then fold and publish all companies.",
)
