CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_address_geocodes_current
(
    address_id FixedString(64),
    address_identity_run_id String,
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
    coordinate_locality Nullable(String),
    coordinate_supporting_point_count UInt32,
    source_record_id Nullable(String),
    source_record_url Nullable(String),
    source_url Nullable(String),
    source_object_key Nullable(String),
    source_md5 Nullable(String),
    source_snapshot_at Nullable(DateTime64(3, 'UTC')),
    source_retrieved_at Nullable(DateTime64(3, 'UTC')),
    geocode_run_id String,
    matched_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY address_id;
