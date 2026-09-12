"""The financial extract job (the Ratsit USD step first, then the four extractors) and its
STOPPED weekly (spec section 8). The fold (slice 3) stays manual, so the weekly stops at the
suggestion layer."""

import dagster as dg

from dagster_v3.defs.se_company.financial.assets import EXTRACTOR_ASSET_NAMES

RATSIT_USD_ASSET = "se_ratsit_financial_periods_usd"
# 5,000 ids per page: every financial page select binds %(company_ids)s two to three times
# (the live CTE, the stored-key read, and Ratsit's periods scan) under ID_BOUND_QUERY_SETTINGS'
# 1 MiB max_query_size; 5,000 twelve-digit ids render to about 65 KB per binding.
WEEKLY_PAGE_SIZE = 5_000
WEEKLY_RUN_CONFIG = {
    "ops": {
        RATSIT_USD_ASSET: {"config": {"execute": True}},
        **{name: {"config": {"execute": True, "page_size": WEEKLY_PAGE_SIZE}} for name in EXTRACTOR_ASSET_NAMES},
    }
}

se_company_financial_extract_job = dg.define_asset_job(
    "se_company_financial_extract_job",
    selection=dg.AssetSelection.assets(RATSIT_USD_ASSET, *EXTRACTOR_ASSET_NAMES),
)
# Monday 07:55 UTC: tests/test_schedule_cron_contracts.py requires a unique (minute, hour)
# and 07:55 was free on 2026-09-12 (the person weekly holds 07:25).
se_company_financial_weekly = dg.ScheduleDefinition(
    name="se_company_financial_weekly",
    job=se_company_financial_extract_job,
    cron_schedule="55 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
