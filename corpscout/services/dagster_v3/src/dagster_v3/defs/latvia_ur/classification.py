"""Latvia UR NACE classification: classify activity texts after ingest.

Latvia publishes no bulk per-company NACE codes (VID keeps them
lookup-only), so activity_text_original is classified semantically via the
shared classifier lib. Results land in corpscout.text_classifications and
surface through the lv_companies_nace view.

This asset requires two GPU-backed endpoints (tagged requires_embedder /
requires_llm): the embedder for retrieval and the LLM for adjudication.
Both default to env vars but are exposed as run config because the hosting
box moves between addresses — an operator can retarget a launchpad run
without editing .env and restarting dagster.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.classifier.lib import (
    EmbeddingClient,
    classify_source,
    llm_call_from_env,
)


class NaceClassificationConfig(dg.Config):
    """Endpoint overrides; every field falls back to its env var when None.

    embed_base_url  -> COMMONCRAWL_EMBED_BASE_URL (OpenAI-compatible /v1)
    llm_base_url    -> TRANSLATION_PROVIDER_LOCAL_BASE_URL
    llm_model       -> TRANSLATION_PROVIDER_LOCAL_MODEL
    """

    embed_base_url: str | None = None
    llm_base_url: str | None = None
    llm_model: str | None = None


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse", "llm"},
    tags={"requires_llm": "true", "requires_embedder": "true"},
    description=(
        "Classify corpscout.lv_companies activity texts into NACE Rev 2.1 via "
        "embedding retrieval over nace_category_embeddings plus LLM "
        "adjudication; cached per distinct text in text_classifications. "
        "Requires the embedder and LLM endpoints to be up (see tags); both "
        "are overridable via run config when the GPU box changes address."
    ),
)
def latvia_ur_nace_classification(
    context: AssetExecutionContext,
    config: NaceClassificationConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    embedder = None
    if config.embed_base_url:
        embedder = EmbeddingClient.from_env(base_url=config.embed_base_url)
    llm_call = llm_model = None
    if config.llm_base_url or config.llm_model:
        llm_call, llm_model = llm_call_from_env(
            base_url=config.llm_base_url, model=config.llm_model
        )

    totals = classify_source(
        context, clickhouse,
        table="corpscout.lv_companies",
        column="activity_text_original",
        embedder=embedder,
        llm_call=llm_call,
        llm_model=llm_model,
    )
    return dg.MaterializeResult(
        metadata={
            "scanned": totals["scanned"],
            "classified": totals["classified"],
            "unknown": totals["unknown"],
        }
    )
