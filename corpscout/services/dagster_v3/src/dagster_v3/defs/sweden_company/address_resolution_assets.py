from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.address_resolution.golden import (
    evaluate_golden_address_resolution_corpus,
)
from dagster_v3.defs.address_resolution.model import (
    GoldenAddressResolutionEvaluation,
)
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_SHADOW_COMPARISON_TABLE,
    QUALIFIED_SHADOW_RESULTS_TABLE,
    replace_sweden_address_resolution_shadow,
)

GROUP_NAME = "sweden_company"
GOLDEN_ASSET_KEY = "sweden_address_resolution_golden_evaluation"
SHADOW_ASSET_KEY = "sweden_address_resolution_shadow_duckdb"
GOLDEN_CORPUS_PATH = (
    Path(__file__).parents[1] / "address_resolution" / "corpora" / "sweden_v1.json"
)


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "test"},
    description=(
        "Evaluates the shared address-resolution engine and Sweden policy against "
        "the versioned Sweden golden corpus. Any mismatch fails materialization."
    ),
)
def sweden_address_resolution_golden_evaluation() -> dg.MaterializeResult:
    evaluation = _evaluate_golden_corpus()
    _raise_for_golden_failures(evaluation)
    return dg.MaterializeResult(metadata=_golden_metadata(evaluation))


@dg.asset(
    deps=[
        dg.AssetKey(GOLDEN_ASSET_KEY),
        dg.AssetKey("sweden_shared_address_osm_matches_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "results_table": QUALIFIED_SHADOW_RESULTS_TABLE,
        "comparison_table": QUALIFIED_SHADOW_COMPARISON_TABLE,
    },
    description=(
        "Builds the reusable resolver's Sweden search indexes and scores all current "
        "shared addresses without changing serving tables, then compares every "
        "outcome with the production matcher."
    ),
)
def sweden_address_resolution_shadow_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    evaluation = _evaluate_golden_corpus()
    _raise_for_golden_failures(evaluation)
    evaluated_at = datetime.now(UTC)
    with sweden_address_osm_duckdb.get_connection() as connection:
        counts = replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id=context.run_id,
            evaluated_at=evaluated_at,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **{
                key: value
                for key, value in counts.items()
                if not isinstance(value, dict | list)
            },
            "shadow_status_counts": dg.MetadataValue.json(
                counts["shadow_status_counts"]
            ),
            "largest_transitions": dg.MetadataValue.json(counts["largest_transitions"]),
            "policy_version": evaluation.policy_version,
            "golden_corpus_version": evaluation.corpus_version,
            "golden_pass_rate_percent": evaluation.pass_rate_percent,
            "evaluated_at": evaluated_at.isoformat(),
        }
    )


def _evaluate_golden_corpus() -> GoldenAddressResolutionEvaluation:
    return evaluate_golden_address_resolution_corpus(
        corpus_path=GOLDEN_CORPUS_PATH,
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
    )


def _raise_for_golden_failures(
    evaluation: GoldenAddressResolutionEvaluation,
) -> None:
    if not evaluation.failures:
        return
    failure_summary = "; ".join(
        (
            f"{failure.case_id}: expected "
            f"{failure.expected_status}/{failure.expected_precision}/"
            f"{failure.expected_strategy}, got "
            f"{failure.actual_status}/{failure.actual_precision}/"
            f"{failure.actual_strategy}"
        )
        for failure in evaluation.failures
    )
    raise ValueError(f"Address-resolution golden corpus failed: {failure_summary}")


def _golden_metadata(
    evaluation: GoldenAddressResolutionEvaluation,
) -> dict[str, object]:
    return {
        "corpus_version": evaluation.corpus_version,
        "policy_version": evaluation.policy_version,
        "case_count": evaluation.case_count,
        "passed_count": evaluation.passed_count,
        "pass_rate_percent": evaluation.pass_rate_percent,
        "failures": dg.MetadataValue.json(
            [
                {
                    "case_id": failure.case_id,
                    "expected_status": failure.expected_status,
                    "actual_status": failure.actual_status,
                    "expected_precision": failure.expected_precision,
                    "actual_precision": failure.actual_precision,
                    "expected_strategy": failure.expected_strategy,
                    "actual_strategy": failure.actual_strategy,
                }
                for failure in evaluation.failures
            ]
        ),
    }


sweden_address_resolution_shadow_job = dg.define_asset_job(
    name="sweden_address_resolution_shadow_job",
    selection=dg.AssetSelection.assets(GOLDEN_ASSET_KEY, SHADOW_ASSET_KEY),
    tags={"country": "SE", "pipeline": "address_resolution_shadow"},
    description=(
        "Gates the shared resolver on Sweden's golden corpus, then evaluates all "
        "current Sweden addresses in shadow mode without changing serving data."
    ),
)


defs = dg.Definitions(
    assets=[
        sweden_address_resolution_golden_evaluation,
        sweden_address_resolution_shadow_duckdb,
    ],
    jobs=[sweden_address_resolution_shadow_job],
)
