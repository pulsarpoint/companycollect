CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_geocodes
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    address_source LowCardinality(String),
    registry_source_record_uid String,
    country_code LowCardinality(String),
    latitude Float64,
    longitude Float64,
    geocode_status LowCardinality(String),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    match_method LowCardinality(String),
    match_confidence Float32,
    candidate_count UInt16,
    coordinate_method LowCardinality(String),
    source_record_id String,
    source_record_url String,
    source_url String,
    source_object_key String,
    source_md5 String,
    source_snapshot_at DateTime64(3, 'UTC'),
    source_retrieved_at DateTime64(3, 'UTC'),
    source_run_id String,
    matched_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, address_key);
