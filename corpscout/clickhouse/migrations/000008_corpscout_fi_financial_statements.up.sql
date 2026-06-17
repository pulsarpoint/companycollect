CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_financial_statements
(
    statement_key String,
    business_id String,
    financial_date Date,
    registration_date Nullable(Date),
    source_url String,
    xml_object_key String,
    xml_sha256 FixedString(64),
    xml_size_bytes UInt64,
    reported_business_id Nullable(String),
    reported_company_name Nullable(String),
    period_start Nullable(Date),
    period_end Nullable(Date),
    contexts_count UInt64,
    units_count UInt64,
    facts_count UInt64,
    validation_warnings String,
    parser_version LowCardinality(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (business_id, financial_date, statement_key);
