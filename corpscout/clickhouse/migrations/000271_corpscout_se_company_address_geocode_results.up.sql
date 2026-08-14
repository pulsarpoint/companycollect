CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_geocode_results
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    address_source LowCardinality(String),
    registry_source_record_uid String,
    street_address String,
    postal_code String,
    post_town String,
    country_code LowCardinality(String),
    normalized_match_key String,
    match_status LowCardinality(String),
    candidate_count UInt16,
    candidate_record_ids Array(String),
    candidate_record_urls Array(String),
    match_method LowCardinality(String),
    match_confidence Float32,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    coordinate_method Nullable(String),
    source_record_id Nullable(String),
    source_record_url Nullable(String),
    source_url Nullable(String),
    source_object_key Nullable(String),
    source_md5 Nullable(String),
    source_snapshot_at Nullable(DateTime64(3, 'UTC')),
    source_retrieved_at Nullable(DateTime64(3, 'UTC')),
    source_run_id String,
    matched_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, address_key);
