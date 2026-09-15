"""The two global People workflows: sync inputs and full processing."""

import dagster as dg

from dagster_v3.defs.se_company.person.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_person_normalize"
MATCH_ASSET = "se_company_person_match"
INPUT_ASSET = "se_company_person_match_input"

# The all-company backoffice workflows. Sync stops at the hashed input snapshots;
# full processing also matches and publishes, in dependency order.
se_company_person_sync_job = dg.define_asset_job(
    "se_company_person_sync_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET, INPUT_ASSET),
    description="Sync ingested source observations into normalized People inputs and hashes. No LLM calls or publishing.",
)
se_company_person_refresh_job = dg.define_asset_job(
    "se_company_person_refresh_job",
    selection=dg.AssetSelection.assets(
        *EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET, INPUT_ASSET, MATCH_ASSET, "se_company_person_publish",
    ),
    description="Full People processing: sync inputs, match with the configured LLM and prompt, then fold and publish.",
)
