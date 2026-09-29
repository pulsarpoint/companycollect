"""Two backoffice operations; source sync never invokes a model or publishes."""

import dagster as dg

from dagster_v3.defs.se_company.domain.tables import EXTRACTOR_ASSETS

SYNC_ASSETS = (*EXTRACTOR_ASSETS, "se_company_domain_precedence_clickhouse")

se_company_domain_sync_job = dg.define_asset_job(
    "se_company_domain_sync_job", selection=dg.AssetSelection.assets(*SYNC_ASSETS),
    description="Sync Brave, Wikidata, ESEF, Common Crawl and saved crawler observations and source precedence. No LLM calls or publishing.",
)
se_company_domain_refresh_job = dg.define_asset_job(
    "se_company_domain_refresh_job",
    selection=dg.AssetSelection.assets(*SYNC_ASSETS, "se_company_domain_publish", "domains_sources", "domains_company_filter"),
    config={"ops": {"domains_sources": {"config": {"source_tables": ["se_company_domain"]}}}},
    description="Publish saved source claims and reviewer decisions with change history. Verification is an independent optional asset and is never run by this job.",
)
