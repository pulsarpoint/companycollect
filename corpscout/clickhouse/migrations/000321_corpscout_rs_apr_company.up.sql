CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per company in every accepted complete APR open-data snapshot.
-- Reprocessing the same snapshot is deduplicated in DuckDB before this table
-- and the current table are rebuilt through staging tables and EXCHANGE TABLES.
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_history
(
    company_id String,
    registration_number String,
    legal_name String,
    municipality_code LowCardinality(String),
    municipality_name_original LowCardinality(String),
    source_status_original LowCardinality(String),
    status LowCardinality(String),
    is_active Bool,
    incorporation_date Date32,
    legal_form_original LowCardinality(String),
    primary_activity_code LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_record_number UInt64,
    source_record_uid FixedString(64),
    state_fingerprint FixedString(64),
    snapshot_date Date32,
    source_url String,
    source_bucket LowCardinality(String),
    source_object_key String,
    updated_from_raw_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT rs_apr_company_history_company_id CHECK match(company_id, '^[0-9]{8}$'),
    CONSTRAINT rs_apr_company_history_registration_number CHECK registration_number = company_id,
    CONSTRAINT rs_apr_company_history_legal_name CHECK trim(legal_name) != '',
    CONSTRAINT rs_apr_company_history_municipality_code CHECK match(municipality_code, '^[0-9]{5}$'),
    CONSTRAINT rs_apr_company_history_status CHECK status IN ('active', 'liquidation', 'bankruptcy', 'compulsory_liquidation'),
    CONSTRAINT rs_apr_company_history_activity_code CHECK match(primary_activity_code, '^[0-9]{4}$'),
    CONSTRAINT rs_apr_company_history_source_record CHECK source_record_id = company_id AND source_record_number > 0,
    CONSTRAINT rs_apr_company_history_dates CHECK incorporation_date <= snapshot_date
)
ENGINE = MergeTree
PARTITION BY toYear(snapshot_date)
ORDER BY (company_id, snapshot_date);

-- One row per company from the newest accepted complete snapshot. A plain
-- MergeTree keeps serving reads simple. The publisher atomically replaces the
-- complete table so removed companies cannot remain as stale current rows.
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company
(
    company_id String,
    registration_number String,
    legal_name String,
    municipality_code LowCardinality(String),
    municipality_name_original LowCardinality(String),
    source_status_original LowCardinality(String),
    status LowCardinality(String),
    is_active Bool,
    incorporation_date Date32,
    legal_form_original LowCardinality(String),
    primary_activity_code LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_record_number UInt64,
    source_record_uid FixedString(64),
    state_fingerprint FixedString(64),
    snapshot_date Date32,
    source_url String,
    source_bucket LowCardinality(String),
    source_object_key String,
    updated_from_raw_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT rs_apr_company_company_id CHECK match(company_id, '^[0-9]{8}$'),
    CONSTRAINT rs_apr_company_registration_number CHECK registration_number = company_id,
    CONSTRAINT rs_apr_company_legal_name CHECK trim(legal_name) != '',
    CONSTRAINT rs_apr_company_municipality_code CHECK match(municipality_code, '^[0-9]{5}$'),
    CONSTRAINT rs_apr_company_status CHECK status IN ('active', 'liquidation', 'bankruptcy', 'compulsory_liquidation'),
    CONSTRAINT rs_apr_company_activity_code CHECK match(primary_activity_code, '^[0-9]{4}$'),
    CONSTRAINT rs_apr_company_source_record CHECK source_record_id = company_id AND source_record_number > 0,
    CONSTRAINT rs_apr_company_dates CHECK incorporation_date <= snapshot_date
)
ENGINE = MergeTree
ORDER BY company_id;
