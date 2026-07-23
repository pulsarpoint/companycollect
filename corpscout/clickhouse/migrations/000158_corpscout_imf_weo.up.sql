CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.imf_weo_vintages
(
    vintage_date Date,
    vintage_label LowCardinality(String),
    dataset_id LowCardinality(String),
    dataset_version LowCardinality(String),
    publication_at DateTime64(3, 'UTC'),
    update_at DateTime64(3, 'UTC'),
    source_url String,
    source_object_key String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY vintage_date;

CREATE TABLE IF NOT EXISTS corpscout.imf_weo_series
(
    vintage_date Date,
    series_code String,
    country_iso3 FixedString(3),
    country_name String,
    indicator_code LowCardinality(String),
    indicator_name String,
    indicator_description String,
    frequency LowCardinality(String),
    scale Nullable(String),
    unit Nullable(String),
    country_update_date Nullable(Date),
    methodology Nullable(String),
    methodology_notes Nullable(String),
    latest_actual_year Nullable(UInt16),
    historical_data_source Nullable(String),
    base_year Nullable(String),
    reporting_year_months Nullable(String),
    chain_weighted Nullable(String),
    basis_of_projections Nullable(String),
    valuation Nullable(String),
    harmonized_prices Nullable(String),
    employment_type Nullable(String),
    government_composition Nullable(String),
    debt_valuation Nullable(String),
    debt_instruments Nullable(String),
    oil_coverage Nullable(String),
    primary_domestic_currency Nullable(String)
)
ENGINE = MergeTree
ORDER BY (vintage_date, country_iso3, indicator_code);

CREATE TABLE IF NOT EXISTS corpscout.imf_weo_observations
(
    vintage_date Date,
    country_iso3 FixedString(3),
    indicator_code LowCardinality(String),
    year UInt16,
    value Float64,
    value_base Float64,
    is_estimate Bool
)
ENGINE = MergeTree
ORDER BY (country_iso3, indicator_code, year, vintage_date);
