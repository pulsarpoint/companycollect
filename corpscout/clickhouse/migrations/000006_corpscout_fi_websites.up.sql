CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_websites
(
    business_id String,
    website_url String,
    website_normalized_url String,
    website_host String,
    root_domain Nullable(String),
    website_path Nullable(String),
    registered_on Nullable(Date),
    ended_on Nullable(Date),
    is_current UInt8,
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (business_id, website_normalized_url);
