CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_addresses_serving_current
(
    company_id String,
    address_id FixedString(64),
    canonical_address_key FixedString(64),
    address_types Array(String),
    address_sources Array(String),
    link_evidence_count UInt32,
    link_first_observed_at DateTime64(3, 'UTC'),
    link_last_observed_at DateTime64(3, 'UTC'),
    review_status LowCardinality(String),
    reviewed_at Nullable(DateTime64(3, 'UTC')),
    reviewed_by String,
    review_note String,
    address_identity_run_id String,
    address_identity_built_at DateTime64(3, 'UTC'),
    serving_run_id String,
    served_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, address_id);
