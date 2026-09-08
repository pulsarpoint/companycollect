"""The address extract job and its STOPPED weekly (spec section 7). The fold stays manual.

The weekly carries the canonical `se_company_address_weekly` name: the old model's schedule
of that name was deleted in slice 4b, so the interim `_v2` name is no longer needed."""

import dagster as dg

from dagster_v3.defs.se_company.address.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_address_normalize"
# The two register scans are the expensive ones: 20,000 ids per page renders inside the
# extract helper's ID_BOUND_QUERY_SETTINGS (one binding per statement).
WEEKLY_PAGE_SIZE = 20_000

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
