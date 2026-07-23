CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.un_comtrade_annual_availability
(
    dataset_code String,
    year UInt16,
    reporter_code UInt16,
    reporter_iso FixedString(3),
    reporter_name String,
    classification_code LowCardinality(String),
    classification_search_code LowCardinality(String),
    is_original_classification Bool,
    has_extended_flow Bool,
    has_extended_partner Bool,
    has_extended_partner2 Bool,
    has_extended_commodity Bool,
    has_extended_customs Bool,
    has_extended_mode_of_transport Bool,
    source_total_records UInt64,
    dataset_checksum String,
    first_released_at DateTime64(3, 'UTC'),
    last_released_at DateTime64(3, 'UTC'),
    source_url String,
    source_object_key String,
    source_object_hash FixedString(64),
    source_run_id String,
    source_line_number UInt64,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (reporter_code, year);

CREATE TABLE IF NOT EXISTS corpscout.un_comtrade_annual_totals
(
    year UInt16,
    reporter_code UInt16,
    reporter_iso FixedString(3),
    reporter_name String,
    flow_code LowCardinality(String),
    flow_name LowCardinality(String),
    classification_code LowCardinality(String),
    classification_search_code LowCardinality(String),
    is_original_classification Bool,
    primary_value_usd Decimal(38, 3),
    cif_value_usd Nullable(Decimal(38, 3)),
    fob_value_usd Nullable(Decimal(38, 3)),
    legacy_estimation_flag Int16,
    is_reported Bool,
    is_aggregate Bool,
    source_url String,
    source_object_key String,
    source_object_hash FixedString(64),
    source_run_id String,
    source_line_number UInt64,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (reporter_code, year, flow_code);
