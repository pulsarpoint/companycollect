from datetime import UTC, datetime
from pathlib import Path

from dbt.cli.main import dbtRunner

from dagster_v3.defs.company_domain_suggestions import tables
from dagster_v3.defs.company_domain_suggestions.dbt_run import (
    complete_sweden_dbt_discovery_run,
)


DBT_DIR = (
    Path(__file__).parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "company_domain_suggestions"
    / "dbt"
)
SHADOW_MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse"
    / "migrations"
    / "000262_corpscout_company_domain_suggestions_dbt.up.sql"
)
IDENTIFIER_MATCH_MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse"
    / "migrations"
    / "000263_corpscout_company_domain_identifier_matches_dbt.up.sql"
)
ADDRESS_NACE_MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse"
    / "migrations"
    / "000266_corpscout_company_domain_address_nace_matching.up.sql"
)


def test_company_domain_suggestion_dbt_project_parses() -> None:
    result = dbtRunner().invoke(
        [
            "parse",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--no-partial-parse",
        ]
    )

    assert result.success, result.exception
    assert result.result is not None
    model_names = {
        node.name
        for node in result.result.nodes.values()
        if node.resource_type.value == "model"
    }
    assert model_names == {
        "stg_se_company_match_features",
        "stg_web_domain_match_features",
        "stg_se_company_domain_identifier_features",
        "int_company_domain_identifier_matches",
        "int_company_domain_identifier_match_classification",
        "int_company_domain_identifier_candidates",
        "int_company_domain_address_matches",
        "int_company_domain_address_nace_matches",
        "int_company_domain_address_nace_candidates",
        "int_company_domain_candidates",
        "company_domain_identifier_matches_dbt",
        "company_domain_suggestions_dbt",
        "company_domain_suggestion_evidence_dbt",
    }


def test_sweden_company_match_features_are_normalized_and_technology_independent() -> None:
    model_sql = (
        DBT_DIR
        / "models"
        / "staging"
        / "stg_se_company_match_features.sql"
    ).read_text()
    identifier_sql = (
        DBT_DIR
        / "models"
        / "staging"
        / "stg_se_company_domain_identifier_features.sql"
    ).read_text()

    for source in (
        "se_companies",
        "se_scb_companies",
        "se_bolagsverket_companies",
        "se_company_addresses_current",
        "se_industries",
        "gleif_lei_records",
    ):
        assert f"source('corpscout', '{source}')" in model_sql

    # The retired registry projection is gone, and the two source_field values it produced
    # are still produced -- by a literal per union branch instead of by a `source` column.
    assert "se_company_registry_current" not in model_sql
    assert "'scb' AS source" in model_sql
    assert "'bolagsverket' AS source" in model_sql
    assert "concat(registry.source, '.legal_name')" in model_sql
    assert "concat(registry.source, '.alternate_name')" in model_sql
    # Only the rows the register still delivers: the publisher tombstones dropped
    # companies with has_company = 0 (owner decision 2026-09-03).
    assert model_sql.count("WHERE has_company = 1") == 2

    assert "normalized_address" in model_sql
    for feature_type in ("identifier", "name", "address", "industry"):
        assert f"'{feature_type}' AS feature_type" in model_sql
    for feature_subtype in ("vat", "lei", "legal_name", "postal", "nace"):
        assert f"'{feature_subtype}' AS feature_subtype" in model_sql
    assert "normalized_value" in model_sql
    assert "raw_value" in model_sql
    assert "materialized='view'" in model_sql
    assert "commoncrawl_" not in model_sql
    assert "root_domain" not in model_sql
    assert "ref('stg_se_company_match_features')" in identifier_sql


def test_web_domain_match_features_are_incremental_and_auditable() -> None:
    model_sql = (
        DBT_DIR
        / "models"
        / "staging"
        / "stg_web_domain_match_features.sql"
    ).read_text()

    for source in (
        "commoncrawl_domains",
        "commoncrawl_domain_identifiers",
        "commoncrawl_page_jsonld",
        "commoncrawl_industries",
    ):
        assert f"source('corpscout', '{source}')" in model_sql

    assert "materialized='incremental'" in model_sql
    assert "incremental_strategy='insert_overwrite'" in model_sql
    assert "unique_key=" not in model_sql
    assert "partition_by=['crawl_id']" in model_sql
    assert "JSONExtractArrayRaw" in model_sql
    assert "JSONType" in model_sql
    assert "normalize_postal_address" in model_sql
    assert "normalize_identity_value('names.raw_value')" in model_sql
    assert "web_feature_crawl_id" in model_sql
    assert "run_query(crawl_query)" in model_sql
    assert "sql_string_literal_list(selected_crawl_ids)" in model_sql
    assert "WHERE crawl_id >" in model_sql
    assert "toString(crawl_id) AS crawl_id" in model_sql
    assert "industry_source_crawls AS" in model_sql
    assert "max(industry_crawls.crawl_id) AS source_crawl_id" in model_sql
    assert "toString(source_crawl_id) AS source_crawl_id" in model_sql
    for feature_type in (
        "identifier",
        "name",
        "address",
        "industry",
        "domain_name",
        "country_tld",
    ):
        assert f"'{feature_type}' AS feature_type" in model_sql
    for evidence_column in (
        "source_crawl_id",
        "raw_value",
        "source_field",
        "source_url",
        "observed_at",
    ):
        assert evidence_column in model_sql


def test_postal_address_macro_matches_the_company_address_contract() -> None:
    macro_sql = (DBT_DIR / "macros" / "identity.sql").read_text()

    assert "macro normalize_postal_address" in macro_sql
    assert "normalizeUTF8NFKC" in macro_sql
    assert r"[^\\p{L}\\p{N}]+" in macro_sql
    assert "00000" in macro_sql
    assert "utlandet" in macro_sql
    assert "component -> component != ''" in macro_sql


def test_deterministic_stage_uses_only_identifiers_or_exact_address_and_nace() -> None:
    model_sql = "\n".join(
        path.read_text()
        for path in sorted((DBT_DIR / "models").rglob("*.sql"))
        if path.name != "stg_web_domain_match_features.sql"
    )
    staging_sql = (
        DBT_DIR
        / "models"
        / "staging"
        / "stg_se_company_domain_identifier_features.sql"
    ).read_text()
    matches_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_identifier_matches.sql"
    ).read_text()

    assert "'vat' AS identifier_type" in staging_sql
    assert "'lei' AS identifier_type" in staging_sql
    assert "lowerUTF8(domains.source_field) = features.identifier_type" in matches_sql
    assert "ref('stg_web_domain_match_features')" not in matches_sql
    for forbidden in (
        "registration_number",
        "organization_name",
        "person_name",
        "domain_label",
        "legal_name_domain_first_token",
        "legal_name_domain_acronym",
        "commoncrawl_industries",
        "commoncrawl_page_jsonld",
    ):
        assert forbidden not in model_sql

    address_matches_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_address_matches.sql"
    ).read_text()
    address_nace_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_address_nace_matches.sql"
    ).read_text()
    combined_candidates_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_candidates.sql"
    ).read_text()

    assert "max_domains_per_address" in address_matches_sql
    assert "feature_type = 'address'" in address_matches_sql
    assert "address_domain_count" in address_matches_sql
    assert "feature_type = 'industry'" in address_nace_sql
    assert "company_nace = domain_nace" in address_nace_sql
    assert "identifier_unique_companies" in combined_candidates_sql
    assert "identifier_unique.root_domain = address_unique.root_domain" in combined_candidates_sql

    strong_signal_test_sql = (
        DBT_DIR / "tests" / "suggestions_use_strong_deterministic_signals.sql"
    ).read_text()
    assert "se-domain-suggestions-dbt-v5" in strong_signal_test_sql
    assert "has(candidate_sources, 'address')" in strong_signal_test_sql
    assert "has(candidate_sources, 'industry')" in strong_signal_test_sql


def test_identifier_fanout_is_classified_before_suggestions() -> None:
    candidates_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_identifier_candidates.sql"
    ).read_text()
    classification_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_identifier_match_classification.sql"
    ).read_text()
    address_candidates_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_address_nace_candidates.sql"
    ).read_text()
    combined_candidates_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_candidates.sql"
    ).read_text()

    assert "max_identifiers_per_domain" in classification_sql
    assert "identifiers_on_domain" in classification_sql
    assert "candidate_domain_count" in candidates_sql
    assert "'directory'" in candidates_sql
    assert "'ambiguous'" in candidates_sql
    assert "'unique'" in candidates_sql
    assert "address_domain_count" in address_candidates_sql
    assert "candidate_domain_count" in address_candidates_sql
    assert "'directory'" in address_candidates_sql
    assert "'ambiguous'" in address_candidates_sql
    assert "'unique'" in address_candidates_sql
    assert "address_score" in combined_candidates_sql
    assert "least(toFloat32(100.0)" in combined_candidates_sql


def test_identifier_directory_detection_counts_all_web_identifiers() -> None:
    classification_sql = (
        DBT_DIR
        / "models"
        / "intermediate"
        / "int_company_domain_identifier_match_classification.sql"
    ).read_text()
    directory_test_sql = (
        DBT_DIR / "tests" / "suggestions_exclude_identifier_directories.sql"
    ).read_text()

    for model_sql in (classification_sql, directory_test_sql):
        normalized_sql = " ".join(model_sql.split())
        assert "source('corpscout', 'web_domain_identity_features')" in normalized_sql
        assert "feature_type = 'identifier'" in normalized_sql
        assert "countDistinct(tuple(" in normalized_sql
        assert "lowerUTF8(features.source_field)" in normalized_sql
        assert "features.normalized_value" in normalized_sql
    assert "INNER JOIN matched_domains USING (root_domain)" in classification_sql
    assert "max_identifiers_per_domain" in directory_test_sql


def test_shadow_tables_are_run_aware_and_migration_owned() -> None:
    migration_sql = SHADOW_MIGRATION.read_text()
    identifier_migration_sql = IDENTIFIER_MATCH_MIGRATION.read_text()
    address_nace_migration_sql = ADDRESS_NACE_MIGRATION.read_text()

    for table in (
        "company_domain_suggestions_dbt",
        "company_domain_suggestion_evidence_dbt",
        "company_domain_dbt_discovery_runs",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in migration_sql
    assert "discovery_run_id String" in migration_sql
    assert "chunk_id UInt16" in migration_sql
    assert "PARTITION BY (country_iso2, toYYYYMM(suggested_at))" in migration_sql
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.company_domain_identifier_matches_dbt"
        in identifier_migration_sql
    )
    for column in (
        "matched_company_count",
        "ambiguous_company_count",
        "directory_only_company_count",
        "unmatched_company_count",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in identifier_migration_sql
    assert "ADD COLUMN IF NOT EXISTS address_score Float32 DEFAULT 0" in (
        address_nace_migration_sql
    )


def test_dbt_history_outputs_replace_the_complete_run_chunk() -> None:
    for model_name in (
        "company_domain_identifier_matches_dbt.sql",
        "company_domain_suggestion_evidence_dbt.sql",
        "company_domain_suggestions_dbt.sql",
    ):
        model_sql = (DBT_DIR / "models" / "marts" / model_name).read_text()
        assert "unique_key=['country_iso2', 'discovery_run_id', 'chunk_id']" in model_sql


class _DbtRunClickHouse:
    def __init__(self) -> None:
        self.run_rows: list[tuple[object, ...]] = []

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> list[tuple[object, ...]]:
        normalized_sql = " ".join(sql.split())
        if normalized_sql.startswith(f"INSERT INTO {tables.QUALIFIED_DBT_RUNS_TABLE}"):
            assert isinstance(params, list)
            self.run_rows.extend(params)
            return []
        if "min(suggested_at)" in normalized_sql:
            return [(datetime(2026, 8, 9, tzinfo=UTC),)]
        if "FROM corpscout.se_companies" in normalized_sql:
            return [(10,)]
        if (
            f"FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE} AS address"
            in normalized_sql
        ):
            return [(1,)]
        if "AS ambiguous" in normalized_sql:
            return [(2,)]
        if "AS candidates" in normalized_sql and "HAVING countIf" in normalized_sql:
            return [(1,)]
        if (
            f"FROM {tables.QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE}" in normalized_sql
            and f"FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}"
            in normalized_sql
            and "uniqExact(company_id)" in normalized_sql
        ):
            return [(8,)]
        if f"FROM {tables.QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE}" in normalized_sql:
            return [(1 if "match_status = 'directory'" in normalized_sql else 3,)]
        if (
            f"FROM {tables.QUALIFIED_DBT_IDENTIFIER_CANDIDATES_TABLE}"
            in normalized_sql
        ):
            return [(1 if "match_status = 'directory'" in normalized_sql else 6,)]
        if f"FROM {tables.QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE}" in normalized_sql:
            return [(6,)]
        if "HAVING count() > 1" in normalized_sql:
            return [(0,)]
        if (
            "INNER JOIN corpscout.company_domain_suggestions AS legacy" in normalized_sql
        ):
            return [(4,)]
        if f"FROM {tables.QUALIFIED_DBT_SUGGESTIONS_TABLE}" in normalized_sql:
            if "identifier_score = 0" in normalized_sql:
                return [(2,)]
            if "identifier_score > 0" in normalized_sql:
                return [(1,)]
            return [(7,)]
        if f"FROM {tables.QUALIFIED_DBT_EVIDENCE_TABLE}" in normalized_sql:
            return [(18,)]
        if f"FROM {tables.QUALIFIED_SUGGESTIONS_TABLE}" in normalized_sql:
            return [(7,)]
        raise AssertionError(sql)


def test_completed_dbt_run_is_recorded_with_parity_metrics() -> None:
    client = _DbtRunClickHouse()

    counts = complete_sweden_dbt_discovery_run(
        client,
        discovery_run_id="dbt-run-1",
        completed_at=datetime(2026, 8, 9, 1, tzinfo=UTC),
    )

    assert counts == {
        "companies": 10,
        "matched_companies": 8,
        "ambiguous_companies": 2,
        "directory_only_companies": 1,
        "unmatched_companies": 2,
        "unresolved_companies": 5,
        "candidate_pairs": 9,
        "disqualified_candidates": 3,
        "identifier_candidate_pairs": 6,
        "address_nace_candidate_pairs": 3,
        "address_nace_suggestions": 2,
        "address_nace_confirmations": 1,
        "address_identifier_conflicts": 1,
        "suggestions": 7,
        "evidence": 18,
        "legacy_suggestions": 7,
        "overlapping_suggestions": 4,
        "legacy_overlap_percentage": 57.143,
        "dbt_overlap_percentage": 57.143,
    }
    assert len(client.run_rows) == 1
    assert client.run_rows[0][0:4] == (
        "SE",
        "dbt-run-1",
        tables.DBT_SCORING_VERSION,
        1,
    )


def test_dbt_assets_and_primary_job_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    job = repository.get_job("sweden_company_domain_suggestions_dbt_job")
    asset_names = {key.path[-1] for key in job.asset_layer.executable_asset_keys}

    assert "company_domain_suggestions_dbt" in asset_names
    assert "company_domain_identifier_matches_dbt" in asset_names
    assert "company_domain_suggestion_evidence_dbt" in asset_names
    assert "int_company_domain_address_matches" in asset_names
    assert "int_company_domain_address_nace_matches" in asset_names
    assert "int_company_domain_address_nace_candidates" in asset_names
    assert "int_company_domain_candidates" in asset_names
    assert "sweden_company_domain_suggestions_dbt_run" in asset_names

    web_job = repository.get_job("company_domain_web_features_dbt_job")
    web_asset_names = {key.path[-1] for key in web_job.asset_layer.executable_asset_keys}
    assert web_asset_names == {"stg_web_domain_match_features"}
    assert web_job.partitions_def is None
