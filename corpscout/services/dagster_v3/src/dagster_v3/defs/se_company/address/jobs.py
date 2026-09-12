"""The address extract job and its STOPPED weekly (spec section 7). The fold stays manual.

The weekly carries the canonical `se_company_address_weekly` name: the old model's schedule
of that name was deleted in slice 4b, so the interim `_v2` name is no longer needed."""

import dagster as dg

from dagster_v3.defs.se_company.address.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_address_normalize"
# The Ratsit page select binds %(company_ids)s THREE times (its report subquery appears in
# both live branches, and the stored-slot read once) and the helper runs every page under
# ID_BOUND_QUERY_SETTINGS' max_query_size of 1 MiB. 20,000 twelve-digit ids render to about
# 300 KB per binding -- ~900 KB in one statement, with no headroom. 10,000 is the number the
# person weekly settled on for its two bindings, and it is what the prod runs use.
WEEKLY_PAGE_SIZE = 10_000

WEEKLY_RUN_CONFIG = {
    "ops": {
        **{name: {"config": {"execute": True, "page_size": WEEKLY_PAGE_SIZE}} for name in EXTRACTOR_ASSET_NAMES},
        NORMALIZE_ASSET: {"config": {"changed_only": True}},
    }
}

se_company_address_extract_job = dg.define_asset_job(
    "se_company_address_extract_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET),
)
se_company_address_weekly = dg.ScheduleDefinition(
    name="se_company_address_weekly",
    job=se_company_address_extract_job,
    cron_schedule="5 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
