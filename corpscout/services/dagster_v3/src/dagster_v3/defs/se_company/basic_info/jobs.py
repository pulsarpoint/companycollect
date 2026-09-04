"""The extract job and its STOPPED weekly schedule (spec 6). The fold stays manual."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.llm import SUGGESTION_PROMPT_VERSION

SQL_EXTRACTOR_ASSETS = (
    "se_basic_info_suggestions_scb",
    "se_basic_info_suggestions_bolagsverket",
    "se_basic_info_suggestions_esef",
    "se_basic_info_suggestions_wikidata",
    "se_basic_info_suggestions_ratsit",
)
LLM_EXTRACTOR_ASSET = "se_basic_info_suggestions_llm"
EXTRACTOR_ASSETS = (*SQL_EXTRACTOR_ASSETS, LLM_EXTRACTOR_ASSET)
# The two register scans are the expensive ones: 20,000 ids per page is 4x fewer pages than
# the default and still renders well inside ID_BOUND_QUERY_SETTINGS' raised max_query_size.
WEEKLY_SQL_PAGE_SIZE = 20_000
# A ceiling on the scheduled LLM spend: an automated weekly must never be the thing that
# discovers how many companies pass the two-source gate.
WEEKLY_LLM_MAX_COMPANIES = 5_000

# Production's pinned model, spelled out because an automated run must never depend on a
# field default and must never be silently downgraded to a preview.
WEEKLY_LLM_PROFILE = {
    "provider": "deepseek",
    "model": "deepseek-v4-flash",
    "base_url": "https://api.deepseek.com",
    "temperature": 0,
    "max_tokens": 6_000,
    "prompt_version": SUGGESTION_PROMPT_VERSION,
    "concurrency": 1,
}
WEEKLY_RUN_CONFIG = {
    "ops": {
        **{
            name: {"config": {"execute": True, "page_size": WEEKLY_SQL_PAGE_SIZE}}
            for name in SQL_EXTRACTOR_ASSETS
        },
        LLM_EXTRACTOR_ASSET: {
            "config": {
                "execute": True,
                "max_companies": WEEKLY_LLM_MAX_COMPANIES,
                "llm": WEEKLY_LLM_PROFILE,
            }
        },
    }
}

se_company_basic_info_extract_job = dg.define_asset_job(
    "se_company_basic_info_extract_job", selection=dg.AssetSelection.assets(*EXTRACTOR_ASSETS)
)
se_company_basic_info_weekly = dg.ScheduleDefinition(
    name="se_company_basic_info_weekly",
    job=se_company_basic_info_extract_job,
    cron_schedule="40 6 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
