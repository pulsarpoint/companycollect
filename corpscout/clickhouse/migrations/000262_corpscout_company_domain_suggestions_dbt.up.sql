CREATE DATABASE IF NOT EXISTS corpscout;

-- Shadow output for the ClickHouse-native dbt implementation. Rows remain append-only by run.
-- application queries must join company_domain_dbt_discovery_runs so partial runs stay invisible.
CREATE TABLE IF NOT EXISTS corpscout.company_domain_suggestions_dbt
(
    country_iso2 LowCardinality(String),
    chunk_id UInt16,
    company_id String,
    root_domain String,
    rank UInt16,
    company_name String,
    candidate_sources Array(LowCardinality(String)),
    identifier_score Float32,
    website_name_score Float32,
    domain_name_score Float32,
    people_score Float32,
    industry_score Float32,
    country_score Float32,
    web_presence_score Float32,
    conflict_penalty Float32,
    total_score Float32,
    scoring_version LowCardinality(String),
    discovery_run_id String,
    suggested_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY (country_iso2, toYYYYMM(suggested_at))
ORDER BY
(
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    rank,
    root_domain
);

CREATE TABLE IF NOT EXISTS corpscout.company_domain_suggestion_evidence_dbt
(
    country_iso2 LowCardinality(String),
    chunk_id UInt16,
    company_id String,
    root_domain String,
    signal_type LowCardinality(String),
    source_field LowCardinality(String),
    company_value String,
    domain_value String,
    score_contribution Float32,
    source_url String,
    crawl_id LowCardinality(String),
    discovery_run_id String,
    suggested_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY (country_iso2, toYYYYMM(suggested_at))
ORDER BY
(
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    root_domain,
    signal_type,
    source_field
);

-- A run becomes reviewable only after dbt models and checks complete and Dagster inserts this row.
CREATE TABLE IF NOT EXISTS corpscout.company_domain_dbt_discovery_runs
(
    country_iso2 LowCardinality(String),
    discovery_run_id String,
    scoring_version LowCardinality(String),
    chunk_count UInt16,
    company_count UInt64,
    candidate_pair_count UInt64,
    disqualified_candidate_count UInt64,
    suggestion_count UInt64,
    evidence_count UInt64,
    legacy_suggestion_count UInt64,
    overlapping_suggestion_count UInt64,
    configuration_json String,
    started_at DateTime64(3, 'UTC'),
    completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
PARTITION BY country_iso2
ORDER BY (country_iso2, discovery_run_id);
