"""The field registry's jobs.

se_company_fields_job is the weekly chain: the three per-source artifacts (they feed the
scb/esef/wikidata extractors), the registry export, the seven candidate extractors and
the resolve. se_company_field_resolve_job is the resolve alone -- what the sensors and
the backoffice launch for a scoped run.

Both subtract se_company_field_parity_check: it is a cutover instrument (compares the
rebuilt wide table with a snapshot the cutover plan creates) and would fail on every
ordinary run. Dagster resolves the subtraction at repository build and refuses an
undefined check key, hence the import of the definition itself. The leaf row-count
check on the resolve asset stays in both jobs.
"""

import dagster as dg

from dagster_v3.defs.se_company.fields.parity import se_company_field_parity_check
from dagster_v3.defs.se_company.fields.resolve import (
    ARTIFACT_ASSETS,
    CANDIDATE_ASSETS,
    REGISTRY_ASSET,
    RESOLVE_ASSET,
)

WEEKLY_ASSETS = (*ARTIFACT_ASSETS, REGISTRY_ASSET, *CANDIDATE_ASSETS, RESOLVE_ASSET)
_PARITY_CHECK = dg.AssetSelection.checks(se_company_field_parity_check)

se_company_field_resolve_job = dg.define_asset_job(
    "se_company_field_resolve_job", selection=dg.AssetSelection.assets(RESOLVE_ASSET) - _PARITY_CHECK)
se_company_fields_job = dg.define_asset_job(
    "se_company_fields_job", selection=dg.AssetSelection.assets(*WEEKLY_ASSETS) - _PARITY_CHECK)

defs = dg.Definitions(jobs=[se_company_field_resolve_job, se_company_fields_job])
