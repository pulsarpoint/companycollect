"""Global company-info source sync and full processing, launched from the backoffice."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.assets import EXTRACTOR_ASSETS, SQL_EXTRACTOR_ASSETS

se_company_basic_info_sync_job = dg.define_asset_job(
    "se_company_basic_info_sync_job",
    selection=dg.AssetSelection.assets(*SQL_EXTRACTOR_ASSETS),
    description="Sync company information from ingested source tables. No LLM calls or publishing.",
)
se_company_basic_info_refresh_job = dg.define_asset_job(
    "se_company_basic_info_refresh_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSETS, "se_company_basic_info_publish"),
    description="Sync company information, synthesize descriptions with the configured LLM, then publish.",
)
