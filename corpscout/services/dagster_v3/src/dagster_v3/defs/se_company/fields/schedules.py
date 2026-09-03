"""The weekly field-registry chain: artifacts -> registry export -> candidates -> resolve.

Takes the Monday 06:50 UTC slot se_company_info_weekly held ((minute, hour) pairs are
unique across every schedule -- tests/test_schedule_cron_contracts.py). STOPPED by
default like its predecessor; the cutover plan starts it on the prod instance once the
rebuild is verified.

The run config spells out what an automated run must never leave to defaults: ``execute``
for the resolve asset AND for every candidate extractor (a bare run of either is a
preview -- plan 2's CandidateExtractConfig gates exactly like the resolve asset), and the
LLM extractor's model profile (spec 5.3: provider and model are required run config, no
default). The registry export and the three artifact assets have no gate.
"""

from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.fields.jobs import se_company_fields_job
from dagster_v3.defs.se_company.fields.resolve import (
    AUTOMATED_RUN_CONFIG,
    CANDIDATE_ASSETS,
    LLM_CANDIDATES_ASSET,
    RESOLVE_ASSET,
)

# Today's production model, pinned here rather than left to the extractor's defaults so
# a default change can never silently change what the weekly run calls. The values are
# info.DEFAULT_LLM_PROFILE's (which the cutover plan deletes with info.py); the key
# names are the LLM extractor's config class -- the Definitions test validates them.
# ``execute`` rides along: without it the extractor previews and writes nothing.
LLM_CANDIDATES_RUN_CONFIG: dict[str, Any] = {
    "execute": True,
    "llm": {
        "provider": "deepseek",
        "model": "deepseek-v4-flash",
        "base_url": "https://api.deepseek.com",
        "temperature": 0,
        "max_tokens": 6_000,
        "prompt_version": "se-company-info-description-v3",
        "concurrency": 1,
    },
}

se_company_fields_weekly = dg.ScheduleDefinition(
    name="se_company_fields_weekly", job=se_company_fields_job, cron_schedule="50 6 * * 1",
    execution_timezone="UTC", default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"ops": {
        RESOLVE_ASSET: {"config": dict(AUTOMATED_RUN_CONFIG)},
        **{name: {"config": dict(AUTOMATED_RUN_CONFIG)}
           for name in CANDIDATE_ASSETS if name != LLM_CANDIDATES_ASSET},
        LLM_CANDIDATES_ASSET: {"config": dict(LLM_CANDIDATES_RUN_CONFIG)},
    }})

defs = dg.Definitions(schedules=[se_company_fields_weekly])
