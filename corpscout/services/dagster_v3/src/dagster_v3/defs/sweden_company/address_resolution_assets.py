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
    SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
    SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
    SWEDEN_STREET_SUFFIX_EXPANSIONS,
    SWEDEN_STREET_VARIANT_LANGUAGES,
)
from dagster_v3.defs.sweden_company.address_resolution_promotion import (
    replace_current_geocodes_from_address_resolution_shadow,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_SHADOW_COMPARISON_TABLE,
    QUALIFIED_SHADOW_RESULTS_TABLE,
    QUALIFIED_UNMATCHED_DIAGNOSTICS_TABLE,
    replace_sweden_address_resolution_shadow,
    replace_sweden_address_resolution_unmatched_diagnostics,
)

GROUP_NAME = "sweden_company"
GOLDEN_ASSET_KEY = "sweden_address_resolution_golden_evaluation"
SHADOW_ASSET_KEY = "sweden_address_resolution_shadow_duckdb"
PROMOTION_ASSET_KEY = "sweden_address_resolution_current_duckdb"
DIAGNOSTICS_ASSET_KEY = "sweden_address_resolution_unmatched_diagnostics_duckdb"
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
        dg.AssetKey("sweden_address_geocode_demand_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "results_table": QUALIFIED_SHADOW_RESULTS_TABLE,
        "comparison_table": QUALIFIED_SHADOW_COMPARISON_TABLE,
    },
    description=(
        "Builds the reusable resolver's Sweden search indexes and scores the address "
        "identities the demand scan selected, without changing serving tables, then "
        "compares every outcome with the one the store already held for it."
    ),
)
def sweden_address_resolution_shadow_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """Score the pending Sweden address identities against the OSM reference index.

    PRECONDITION: sweden_address_geocode_demand_duckdb must have run against this same
    DuckDB at least once. It writes se_address_pending_identities, which this asset
    reads first; on a DuckDB where the demand asset has never run, that read fails with
    a DuckDB CatalogException naming that table, and the fix is to materialize the
    demand asset, not to repair anything here.
    """
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


@dg.asset(
    deps=[dg.AssetKey(SHADOW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    description=(
        "Promotes one complete policy-compatible Sweden resolver shadow run to "
        "the live DuckDB geocode table after validating address coverage, run "
        "identity, coordinate precision, and OSM provenance."
    ),
)
def sweden_address_resolution_current_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """Promote the shadow run's outcomes for the pending identities to the serving table.

    Carries the same precondition as the shadow: se_address_pending_identities must
    exist, or this raises a DuckDB CatalogException naming it. With nothing pending it
    promotes nothing and leaves both the serving and hand-off tables untouched.
    """
    matched_at = datetime.now(UTC)
    with sweden_address_osm_duckdb.get_connection() as connection:
        counts = replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id=context.run_id,
            matched_at=matched_at,
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **{
                key: value
                for key, value in counts.items()
                if not isinstance(value, dict | list)
            },
            "status_counts": dg.MetadataValue.json(counts["status_counts"]),
            "matched_at": matched_at.isoformat(),
        }
    )


@dg.asset(
    deps=[dg.AssetKey(SHADOW_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": QUALIFIED_UNMATCHED_DIAGNOSTICS_TABLE},
    description=(
        "Classifies every unmatched Sweden address with parser, OSM coverage, "
        "address-context, and nearest typo-candidate evidence for review and "
        "resolver tuning."
    ),
)
def sweden_address_resolution_unmatched_diagnostics_duckdb(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    diagnosed_at = datetime.now(UTC)
    with sweden_address_osm_duckdb.get_connection() as connection:
        counts = replace_sweden_address_resolution_unmatched_diagnostics(
            connection=connection,
            diagnosed_at=diagnosed_at,
        )
    return dg.MaterializeResult(
        metadata={
            **{
                key: value
                for key, value in counts.items()
                if not isinstance(value, dict | list)
            },
            "reason_counts": dg.MetadataValue.json(counts["reason_counts"]),
            "diagnosed_at": diagnosed_at.isoformat(),
        }
    )


def _evaluate_golden_corpus() -> GoldenAddressResolutionEvaluation:
    return evaluate_golden_address_resolution_corpus(
        corpus_path=GOLDEN_CORPUS_PATH,
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        street_variant_languages_by_country=(SWEDEN_STREET_VARIANT_LANGUAGES),
        street_suffix_expansions_by_country=(SWEDEN_STREET_SUFFIX_EXPANSIONS),
        exact_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
        separate_definite_by_country=SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
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
        "Gates the shared resolver on Sweden's golden corpus, then evaluates the "
        "pending Sweden address identities in shadow mode without changing serving "
        "data. This job does not include the demand asset, so running it alone "
        "matches whatever pending set the DuckDB already holds."
    ),
)

sweden_address_resolution_publish_job = dg.define_asset_job(
    name="sweden_address_resolution_publish_job",
    selection=dg.AssetSelection.assets(
        PROMOTION_ASSET_KEY,
        "sweden_address_geocode_store_clickhouse",
    ),
    tags={"country": "SE", "pipeline": "address_resolution_publish"},
    description=(
        "Promotes the already-materialized, policy-gated Sweden resolver shadow "
        "results and publishes them atomically to ClickHouse."
    ),
)

sweden_address_resolution_diagnostics_job = dg.define_asset_job(
    name="sweden_address_resolution_diagnostics_job",
    selection=dg.AssetSelection.assets(DIAGNOSTICS_ASSET_KEY),
    tags={"country": "SE", "pipeline": "address_resolution_diagnostics"},
    description=(
        "Materializes one reviewable reason and the nearest indexed evidence for "
        "every unmatched Sweden address from the current resolver shadow run."
    ),
)


defs = dg.Definitions(
    assets=[
        sweden_address_resolution_golden_evaluation,
        sweden_address_resolution_shadow_duckdb,
        sweden_address_resolution_current_duckdb,
        sweden_address_resolution_unmatched_diagnostics_duckdb,
    ],
    jobs=[
        sweden_address_resolution_shadow_job,
        sweden_address_resolution_publish_job,
        sweden_address_resolution_diagnostics_job,
    ],
)
