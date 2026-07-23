CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.world_bank_macro_observations
(
    country_code LowCardinality(String),
    country_iso3 FixedString(3),
    country_name String,
    region LowCardinality(String),
    income_group LowCardinality(String),
    indicator_code LowCardinality(String),
    indicator_name String,
    year UInt16,
    value Float64,
    source LowCardinality(String),
    source_dataset LowCardinality(String),
    source_updated_date Date,
    source_url String,
    source_object_key String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (country_code, indicator_code, year);
