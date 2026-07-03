"""Latvia UR NACE classification: classify activity texts after ingest.

Latvia publishes no bulk per-company NACE codes (VID keeps them
lookup-only), so activity_text_original is classified semantically via the
shared classifier lib. Results land in corpscout.text_classifications and
surface through the lv_companies_nace view.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.classifier.lib import classify_source


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Classify corpscout.lv_companies activity texts into NACE Rev 2.1 via "
        "embedding retrieval over nace_category_embeddings plus LLM "
        "adjudication; cached per distinct text in text_classifications."
    ),
)
def latvia_ur_nace_classification(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = classify_source(
        context, clickhouse,
        table="corpscout.lv_companies",
        column="activity_text_original",
    )
    return dg.MaterializeResult(
        metadata={
            "scanned": totals["scanned"],
            "classified": totals["classified"],
            "unknown": totals["unknown"],
        }
    )
