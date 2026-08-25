-- Reverts 000319. Structure only -- the rows are not restored, and no asset writes them
-- any more. Restoring them means reverting the code change that deleted
-- defs/sweden_company/address_geocoding.py and its three assets, then re-materializing the
-- weekly geocoding job.
--
-- se_company_address_geocodes is recreated verbatim from 000270.
-- se_company_address_geocode_results is recreated verbatim from 000271 with the three
-- coordinate columns 000272 and 000277 added, in the positions those ALTERs put them.

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
    coordinate_locality Nullable(String),
    coordinate_supporting_point_count UInt32,
    coordinate_spread_meters Nullable(Float64),
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
