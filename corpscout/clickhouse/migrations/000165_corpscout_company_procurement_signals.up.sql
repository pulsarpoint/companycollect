CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.company_government_contract_evidence
(
    country_code LowCardinality(String),
    company_id String,
    evidence_id String,
    source_slugs Array(String),
    source_references Array(String),
    publication_date Nullable(Date),
    buyer_name String,
    title String,
    agreement_type LowCardinality(String),
    source_updated_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (country_code, company_id, evidence_id);

CREATE TABLE IF NOT EXISTS corpscout.company_public_procurement_summary
(
    country_code LowCardinality(String),
    company_id String,
    public_award_count UInt32,
    public_award_last_date Nullable(Date),
    source_slugs Array(String),
    source_updated_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (country_code, company_id);

CREATE TABLE IF NOT EXISTS corpscout.company_signal_coverage
(
    country_code LowCardinality(String),
    signal_name LowCardinality(String),
    coverage_status LowCardinality(String),
    coverage_from Nullable(Date),
    coverage_to Nullable(Date),
    source_slugs Array(String),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC'),
    caveat String
)
ENGINE = MergeTree
ORDER BY (country_code, signal_name);
