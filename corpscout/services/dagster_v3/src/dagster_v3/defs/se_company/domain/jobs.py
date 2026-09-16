"""Two backoffice operations; source sync never invokes a model or publishes."""

import dagster as dg

from dagster_v3.defs.se_company.domain.tables import EXTRACTOR_ASSETS

SYNC_ASSETS = (*EXTRACTOR_ASSETS, "se_company_domain_precedence_clickhouse")

se_company_domain_sync_job = dg.define_asset_job(
    "se_company_domain_sync_job", selection=dg.AssetSelection.assets(*SYNC_ASSETS),
    description="Sync Brave, Wikidata, ESEF and Common Crawl observations and source precedence. No LLM calls or publishing.",
)
se_company_domain_refresh_job = dg.define_asset_job(
    "se_company_domain_refresh_job",
    selection=dg.AssetSelection.assets(*SYNC_ASSETS, "se_company_domain_verification", "se_company_domain_publish"),
    description="Sync domain evidence, optionally verify uncertain or conflicting associations, and fold all companies with change history.",
)
