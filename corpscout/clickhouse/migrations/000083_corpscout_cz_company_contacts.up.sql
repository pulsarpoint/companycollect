CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.cz_company_contacts
(
    source_slug LowCardinality(String),
    source_record_id String,
    ico String,
    contact_type LowCardinality(String),
    contact_value String,
    domain String,
    domain_source LowCardinality(String),
    confidence Float32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (ico, contact_type, contact_value);
