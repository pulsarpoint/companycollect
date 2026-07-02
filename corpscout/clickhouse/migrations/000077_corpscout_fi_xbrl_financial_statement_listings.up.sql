CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_financial_statement_listings
(
    business_id String,
    financial_date Date,
    registration_date Date,
    source_system LowCardinality(String) DEFAULT 'finland_prh_xbrl',
    source_run_id String DEFAULT '',
    source_record_id String DEFAULT '',
    source_payload_hash Nullable(FixedString(64)) DEFAULT NULL,
    resolved_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (business_id, financial_date, registration_date);
