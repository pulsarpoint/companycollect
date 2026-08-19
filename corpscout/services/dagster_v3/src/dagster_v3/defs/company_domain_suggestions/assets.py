from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.duckdb_resources import (
    read_only_duckdb_connection,
    safe_duckdb_connection,
)
from dagster_v3.defs.company_domain_suggestions import scoring, tables
from dagster_v3.defs.company_domain_suggestions.dbt_run import (
    complete_sweden_dbt_discovery_run,
)
from dagster_v3.defs.company_domain_suggestions.feature_index import (
    replace_web_domain_identity_features,
)
from dagster_v3.defs.company_domain_suggestions.inputs import (
    replace_sweden_suggestion_inputs,
)
from dagster_v3.defs.company_domain_suggestions.publish import (
    publish_country_suggestions,
)

GROUP_NAME = "company_domain_suggestions"
COUNTRY_PARTITIONS = dg.StaticPartitionsDefinition([tables.COUNTRY_ISO2])


class WebDomainIdentityFeaturesConfig(dg.Config):
    include_jsonld: bool = True
    progress_log_interval_seconds: float = 30.0


class SwedenCompanyDomainSuggestionsConfig(dg.Config):
    query_batch_size: int = 10_000
    progress_log_interval_seconds: float = 30.0


class PublishCompanyDomainSuggestionsConfig(dg.Config):
    allow_empty: bool = False
    insert_batch_size: int = 50_000


class CompleteDbtCompanyDomainSuggestionsConfig(dg.Config):
    allow_empty: bool = False


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"clickhouse", "commoncrawl"},
    metadata={"table": tables.QUALIFIED_FEATURES_TABLE},
    description=(
        "Builds the reverse Common Crawl identity index used for bounded company-domain "
        "candidate generation. JSON-LD people and organization names are included by default."
    ),
)
def web_domain_identity_features_clickhouse(
    context: dg.AssetExecutionContext,
    config: WebDomainIdentityFeaturesConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    source_tables = [
        tables.FEATURES_TABLE,
        "commoncrawl_domains",
        "commoncrawl_domain_identifiers",
    ]
    if config.include_jsonld:
        source_tables.append("commoncrawl_page_jsonld")
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=tuple(source_tables),
    )
    with clickhouse.get_connection() as client:
        counts = replace_web_domain_identity_features(
            client,
            indexed_at=datetime.now(UTC),
            include_jsonld=config.include_jsonld,
            progress_log_interval_seconds=config.progress_log_interval_seconds,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"table": tables.QUALIFIED_FEATURES_TABLE, **counts}
    )


@dg.asset(
    deps=["web_domain_identity_features_clickhouse"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=COUNTRY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    description=(
        "Stages Sweden company identifiers, bounded name/person features, and matched web "
        "evidence in DuckDB, then produces a scored suggestion snapshot."
    ),
)
def sweden_company_domain_suggestions_duckdb(
    context: dg.AssetExecutionContext,
    config: SwedenCompanyDomainSuggestionsConfig,
    company_domain_suggestions_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    if context.partition_key != tables.COUNTRY_ISO2:
        raise ValueError(
            "The first company-domain suggestion implementation only supports Sweden"
        )
    started_at = datetime.now(UTC)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.FEATURES_TABLE,
            "se_companies",
            "se_financial_report_signatories",
            "se_industries",
            "gleif_lei_records",
            "commoncrawl_page_jsonld",
            "commoncrawl_industries",
            "commoncrawl_domain_identifiers",
        ),
    )
    with (
        safe_duckdb_connection(company_domain_suggestions_duckdb) as connection,
        clickhouse.get_connection() as client,
    ):
        input_counts = replace_sweden_suggestion_inputs(
            connection,
            client,
            query_batch_size=config.query_batch_size,
            progress_log_interval_seconds=config.progress_log_interval_seconds,
            log=context.log.info,
        )
        score_counts = scoring.replace_scored_suggestions(
            connection,
            discovery_run_id=context.run.run_id,
            suggested_at=started_at,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "country_iso2": context.partition_key,
            "duckdb_path": str(tables.DUCKDB_PATH),
            "scoring_version": tables.SCORING_VERSION,
            "started_at": started_at.isoformat(),
            **input_counts,
            **score_counts,
        }
    )


@dg.asset(
    deps=["sweden_company_domain_suggestions_duckdb"],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    partitions_def=COUNTRY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_SUGGESTIONS_TABLE},
    description=(
        "Atomically publishes the Sweden suggestion/evidence country snapshot and appends "
        "successful run provenance. It does not create canonical company-domain facts."
    ),
)
def sweden_company_domain_suggestions_clickhouse(
    context: dg.AssetExecutionContext,
    config: PublishCompanyDomainSuggestionsConfig,
    company_domain_suggestions_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    if context.partition_key != tables.COUNTRY_ISO2:
        raise ValueError(
            "The first company-domain suggestion implementation only supports Sweden"
        )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.SUGGESTIONS_TABLE, tables.EVIDENCE_TABLE, tables.RUNS_TABLE),
    )
    completed_at = datetime.now(UTC)
    with (
        read_only_duckdb_connection(company_domain_suggestions_duckdb) as connection,
        clickhouse.get_connection() as client,
    ):
        started_at_row = connection.execute(
            f"select min(suggested_at) from {tables.DUCKDB_SCHEMA}.{tables.SUGGESTIONS_TABLE}"
        ).fetchone()
        started_at = (
            started_at_row[0]
            if started_at_row is not None and started_at_row[0] is not None
            else completed_at
        )
        counts = publish_country_suggestions(
            connection,
            client,
            discovery_run_id=context.run.run_id,
            started_at=started_at,
            completed_at=completed_at,
            allow_empty=config.allow_empty,
            batch_size=config.insert_batch_size,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "country_iso2": context.partition_key,
            "suggestions_table": tables.QUALIFIED_SUGGESTIONS_TABLE,
            "evidence_table": tables.QUALIFIED_EVIDENCE_TABLE,
            "runs_table": tables.QUALIFIED_RUNS_TABLE,
            "scoring_version": tables.SCORING_VERSION,
            **counts,
        }
    )


@dg.asset(
    deps=[
        "company_domain_identifier_matches_dbt",
        "company_domain_suggestions_dbt",
        "company_domain_suggestion_evidence_dbt",
    ],
    group_name="company_domain_suggestions_dbt",
    kinds={"dbt", "clickhouse"},
    partitions_def=COUNTRY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="company_domain_suggestions_clickhouse_dbt",
    metadata={"table": tables.QUALIFIED_DBT_RUNS_TABLE},
    description=(
        "Validates completed Sweden identifier and exact address-plus-NACE matches, records "
        "the activation marker, and reports resolution and conflict counts."
    ),
)
def sweden_company_domain_suggestions_dbt_run(
    context: dg.AssetExecutionContext,
    config: CompleteDbtCompanyDomainSuggestionsConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    if context.partition_key != tables.COUNTRY_ISO2:
        raise ValueError("The first dbt company-domain implementation only supports Sweden")
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.DBT_SUGGESTIONS_TABLE,
            tables.DBT_EVIDENCE_TABLE,
            tables.DBT_RUNS_TABLE,
            tables.DBT_IDENTIFIER_MATCHES_TABLE,
            tables.DBT_IDENTIFIER_CANDIDATES_TABLE,
            tables.DBT_ADDRESS_NACE_CANDIDATES_TABLE,
            tables.DBT_COMBINED_CANDIDATES_TABLE,
        ),
    )
    completed_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        counts = complete_sweden_dbt_discovery_run(
            client,
            discovery_run_id=context.run_id,
            completed_at=completed_at,
            allow_empty=config.allow_empty,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "country_iso2": context.partition_key,
            "discovery_run_id": context.run_id,
            "scoring_version": tables.DBT_SCORING_VERSION,
            "completed_at": completed_at.isoformat(),
            "suggestions_table": tables.QUALIFIED_DBT_SUGGESTIONS_TABLE,
            "evidence_table": tables.QUALIFIED_DBT_EVIDENCE_TABLE,
            "identifier_matches_table": (
                tables.QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE
            ),
            "runs_table": tables.QUALIFIED_DBT_RUNS_TABLE,
            **counts,
        }
    )


web_domain_identity_features_job = dg.define_asset_job(
    "web_domain_identity_features_job",
    selection=dg.AssetSelection.assets("web_domain_identity_features_clickhouse"),
)

company_domain_web_features_dbt_job = dg.define_asset_job(
    "company_domain_web_features_dbt_job",
    selection=dg.AssetSelection.groups("company_domain_web_features_dbt"),
)

sweden_company_domain_suggestions_dbt_job = dg.define_asset_job(
    "sweden_company_domain_suggestions_dbt_job",
    selection=(
        dg.AssetSelection.groups("company_domain_suggestions_dbt")
        | dg.AssetSelection.assets("sweden_company_domain_suggestions_dbt_run")
    ),
    partitions_def=COUNTRY_PARTITIONS,
)


defs = dg.Definitions(
    assets=[
        web_domain_identity_features_clickhouse,
        sweden_company_domain_suggestions_dbt_run,
    ],
    jobs=[
        web_domain_identity_features_job,
        company_domain_web_features_dbt_job,
        sweden_company_domain_suggestions_dbt_job,
    ],
)
