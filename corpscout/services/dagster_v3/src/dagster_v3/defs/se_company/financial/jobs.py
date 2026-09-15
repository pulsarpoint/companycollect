"""Global financial source sync and full processing, launched from the backoffice."""

import dagster as dg

from dagster_v3.defs.se_company.financial.assets import EXTRACTOR_ASSET_NAMES

RATSIT_USD_ASSET = "se_ratsit_financial_periods_usd"

se_company_financial_sync_job = dg.define_asset_job(
    "se_company_financial_sync_job",
    selection=dg.AssetSelection.assets(RATSIT_USD_ASSET, *EXTRACTOR_ASSET_NAMES),
    description="Convert ingested Ratsit figures to USD and sync the four financial suggestion sources.",
)
se_company_financial_refresh_job = dg.define_asset_job(
    "se_company_financial_refresh_job",
    selection=dg.AssetSelection.assets(RATSIT_USD_ASSET, *EXTRACTOR_ASSET_NAMES, "se_company_financial_publish"),
    description="Sync financial inputs, then fold and publish all companies. No LLM calls.",
)
