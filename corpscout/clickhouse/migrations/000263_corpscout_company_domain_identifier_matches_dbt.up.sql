CREATE DATABASE IF NOT EXISTS corpscout;

-- Exact VAT and LEI matches remain auditable even when fan-out prevents publication.
CREATE TABLE IF NOT EXISTS corpscout.company_domain_identifier_matches_dbt
(
    country_iso2 LowCardinality(String),
    chunk_id UInt16,
    company_id String,
    company_name String,
    root_domain String,
    identifier_type LowCardinality(String),
    normalized_identifier String,
    company_value String,
    domain_value String,
    source_url String,
    crawl_id LowCardinality(String),
    identifiers_on_domain UInt32,
    domains_for_identifier UInt32,
    candidate_domain_count UInt32,
    match_status LowCardinality(String),
    scoring_version LowCardinality(String),
    discovery_run_id String,
    matched_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY (country_iso2, toYYYYMM(matched_at))
ORDER BY
(
    country_iso2,
    discovery_run_id,
    chunk_id,
    company_id,
    root_domain,
    identifier_type,
    normalized_identifier
);

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    ADD COLUMN IF NOT EXISTS matched_company_count UInt64 DEFAULT 0
    AFTER company_count;

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    ADD COLUMN IF NOT EXISTS ambiguous_company_count UInt64 DEFAULT 0
    AFTER matched_company_count;

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    ADD COLUMN IF NOT EXISTS directory_only_company_count UInt64 DEFAULT 0
    AFTER ambiguous_company_count;

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    ADD COLUMN IF NOT EXISTS unmatched_company_count UInt64 DEFAULT 0
    AFTER directory_only_company_count;
