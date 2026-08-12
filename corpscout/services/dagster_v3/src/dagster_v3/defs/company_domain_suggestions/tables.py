from pathlib import Path

CLICKHOUSE_DATABASE = "corpscout"
FEATURES_TABLE = "web_domain_identity_features"
SUGGESTIONS_TABLE = "company_domain_suggestions"
EVIDENCE_TABLE = "company_domain_suggestion_evidence"
RUNS_TABLE = "company_domain_discovery_runs"
DBT_SUGGESTIONS_TABLE = "company_domain_suggestions_dbt"
DBT_EVIDENCE_TABLE = "company_domain_suggestion_evidence_dbt"
DBT_RUNS_TABLE = "company_domain_dbt_discovery_runs"
DBT_IDENTIFIER_MATCHES_TABLE = "company_domain_identifier_matches_dbt"
DBT_IDENTIFIER_CANDIDATES_TABLE = "int_company_domain_identifier_candidates"
DBT_ADDRESS_NACE_CANDIDATES_TABLE = "int_company_domain_address_nace_candidates"
DBT_COMBINED_CANDIDATES_TABLE = "int_company_domain_candidates"

QUALIFIED_FEATURES_TABLE = f"{CLICKHOUSE_DATABASE}.{FEATURES_TABLE}"
QUALIFIED_SUGGESTIONS_TABLE = f"{CLICKHOUSE_DATABASE}.{SUGGESTIONS_TABLE}"
QUALIFIED_EVIDENCE_TABLE = f"{CLICKHOUSE_DATABASE}.{EVIDENCE_TABLE}"
QUALIFIED_RUNS_TABLE = f"{CLICKHOUSE_DATABASE}.{RUNS_TABLE}"
QUALIFIED_DBT_SUGGESTIONS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{DBT_SUGGESTIONS_TABLE}"
)
QUALIFIED_DBT_EVIDENCE_TABLE = f"{CLICKHOUSE_DATABASE}.{DBT_EVIDENCE_TABLE}"
QUALIFIED_DBT_RUNS_TABLE = f"{CLICKHOUSE_DATABASE}.{DBT_RUNS_TABLE}"
QUALIFIED_DBT_IDENTIFIER_MATCHES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{DBT_IDENTIFIER_MATCHES_TABLE}"
)
QUALIFIED_DBT_IDENTIFIER_CANDIDATES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{DBT_IDENTIFIER_CANDIDATES_TABLE}"
)
QUALIFIED_DBT_ADDRESS_NACE_CANDIDATES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{DBT_ADDRESS_NACE_CANDIDATES_TABLE}"
)
QUALIFIED_DBT_COMBINED_CANDIDATES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{DBT_COMBINED_CANDIDATES_TABLE}"
)

DUCKDB_SCHEMA = "company_domain_suggestions"
DUCKDB_PATH = Path("data/company_domain_suggestion_staging.duckdb")
DUCKDB_POOL = "company_domain_suggestions_duckdb"

SCORING_VERSION = "se-domain-suggestions-v1"
DBT_SCORING_VERSION = "se-domain-suggestions-dbt-v5"
MAX_IDENTIFIERS_PER_DOMAIN = 5
MAX_DOMAINS_PER_ADDRESS = 5
COUNTRY_ISO2 = "SE"

FEATURE_COLUMNS = (
    "feature_type",
    "normalized_value",
    "root_domain",
    "raw_value",
    "source_field",
    "source_url",
    "crawl_id",
    "source_resolved_at",
    "indexed_at",
)

SUGGESTION_COLUMNS = (
    "country_iso2",
    "company_id",
    "root_domain",
    "rank",
    "company_name",
    "candidate_sources",
    "identifier_score",
    "website_name_score",
    "domain_name_score",
    "people_score",
    "industry_score",
    "country_score",
    "web_presence_score",
    "conflict_penalty",
    "total_score",
    "scoring_version",
    "discovery_run_id",
    "suggested_at",
)

EVIDENCE_COLUMNS = (
    "country_iso2",
    "company_id",
    "root_domain",
    "signal_type",
    "source_field",
    "company_value",
    "domain_value",
    "score_contribution",
    "source_url",
    "crawl_id",
    "discovery_run_id",
    "suggested_at",
)

RUN_COLUMNS = (
    "country_iso2",
    "discovery_run_id",
    "scoring_version",
    "company_count",
    "candidate_pair_count",
    "disqualified_candidate_count",
    "suggestion_count",
    "evidence_count",
    "configuration_json",
    "started_at",
    "completed_at",
)
