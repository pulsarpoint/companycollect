"""The person extract job (extractors, normalize, match) and its STOPPED weekly.

The fold stays manual (slice 2), so the weekly stops at the match layer.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_person_normalize"
MATCH_ASSET = "se_company_person_match"
# Half the address entity's page. Every person page select binds %(company_ids)s twice (the
# live CTE and the stored-slot read) and the helper runs it under ID_BOUND_QUERY_SETTINGS'
# max_query_size of 1 MiB; 10,000 twelve-digit ids render to about 130 KB per binding, so
# the rendered statement stays well inside it.
WEEKLY_PAGE_SIZE = 10_000

WEEKLY_RUN_CONFIG = {
    "ops": {
        **{
            name: {"config": {"execute": True, "page_size": WEEKLY_PAGE_SIZE}}
            for name in EXTRACTOR_ASSET_NAMES
        },
        NORMALIZE_ASSET: {"config": {"changed_only": True}},
        # provider and model are spelled out because the match profile has no defaults for
        # them: an automated run must say which model it is paying for.
        MATCH_ASSET: {
            "config": {
                "provider": "deepseek",
                "model": "deepseek-v4-flash",
                "changed_only": True,
            }
        },
    }
}

se_company_person_extract_job = dg.define_asset_job(
    "se_company_person_extract_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET, MATCH_ASSET),
)
# Spec section 6 asks for Monday 07:15 UTC; 07:15 belongs to
# france_sirene_register_schedule and tests/test_schedule_cron_contracts.py requires a
# unique (minute, hour), so this is the next free minute of the same hour.
se_company_person_weekly = dg.ScheduleDefinition(
    name="se_company_person_weekly",
    job=se_company_person_extract_job,
    cron_schedule="25 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
