"""The extract job and its STOPPED weekly schedule (spec 6). The fold stays manual."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.llm import SUGGESTION_PROMPT_VERSION

EXTRACTOR_ASSETS = (
    "se_basic_info_suggestions_scb",
    "se_basic_info_suggestions_bolagsverket",
    "se_basic_info_suggestions_esef",
    "se_basic_info_suggestions_wikidata",
    "se_basic_info_suggestions_ratsit",
    "se_basic_info_suggestions_llm",
)

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
        **{name: {"config": {"execute": True}} for name in EXTRACTOR_ASSETS[:-1]},
        "se_basic_info_suggestions_llm": {"config": {"execute": True, "llm": WEEKLY_LLM_PROFILE}},
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
