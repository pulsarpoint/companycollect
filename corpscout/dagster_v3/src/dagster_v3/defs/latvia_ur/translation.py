"""Latvia UR translation loader: what to translate and how, for this source.

Source-specific translation knowledge lives here, next to the ingest assets:
one LLM-translated free-text column, no static maps. The generic
scan/enqueue machinery is imported from
``dagster_v3.defs.translator_load.loader``.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.translator_load.loader import (
    LoaderField,
    LoaderSource,
    load_source,
    stats_check,
)

_LATVIA = LoaderSource(
    source_lang="lv",
    target_lang="en",
    source_language_name="Latvian",
    target_language_name="English",
    fields=(LoaderField("corpscout.lv_companies", "activity_text_original"),),
)


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.lv_companies for untranslated texts and enqueue them "
        "to the translator service."
    ),
)
def latvia_ur_translation_load(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = load_source(context, clickhouse, _LATVIA)
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": totals["received"],
            "enqueued_inserted": totals["inserted"],
        }
    )


@dg.asset_check(asset=latvia_ur_translation_load, name="translator_stats_reachable")
def latvia_ur_translator_stats_check() -> dg.AssetCheckResult:
    return stats_check()
