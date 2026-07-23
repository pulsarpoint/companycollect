CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.eurostat_datasets
(
    dataset_code LowCardinality(String),
    title String,
    dsd_version LowCardinality(String),
    source_observation_count UInt64,
    source_oldest_period LowCardinality(String),
    source_latest_period LowCardinality(String),
    data_updated_at DateTime64(3, 'UTC'),
    structure_updated_at DateTime64(3, 'UTC'),
    source_data_url String,
    source_data_object_key String,
    source_data_hash FixedString(64),
    source_structure_url String,
    source_structure_object_key String,
    source_structure_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY dataset_code;

CREATE TABLE IF NOT EXISTS corpscout.eurostat_dimension_values
(
    dataset_code LowCardinality(String),
    dimension_code LowCardinality(String),
    dimension_label String,
    dimension_position UInt16,
    value_code String,
    value_label String,
    value_position UInt16
)
ENGINE = MergeTree
ORDER BY (dataset_code, dimension_code, value_code);

CREATE TABLE IF NOT EXISTS corpscout.eurostat_series
(
    dataset_code LowCardinality(String),
    series_key String,
    geo_code LowCardinality(String),
    frequency LowCardinality(String),
    unit_code Nullable(String),
    source_line_number UInt64
)
ENGINE = MergeTree
ORDER BY (dataset_code, geo_code, series_key);

CREATE TABLE IF NOT EXISTS corpscout.eurostat_series_dimensions
(
    dataset_code LowCardinality(String),
    series_key String,
    dimension_code LowCardinality(String),
    value_code String,
    dimension_position UInt16
)
ENGINE = MergeTree
ORDER BY (dataset_code, series_key, dimension_position);

CREATE TABLE IF NOT EXISTS corpscout.eurostat_observations
(
    dataset_code LowCardinality(String),
    geo_code LowCardinality(String),
    series_key String,
    time_period LowCardinality(String),
    period_start Date,
    year UInt16,
    value Nullable(Float64),
    status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (dataset_code, geo_code, series_key, period_start);
