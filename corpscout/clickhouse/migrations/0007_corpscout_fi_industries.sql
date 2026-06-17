CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_industries
(
    business_id String,
    source_industry_code Nullable(String),
    source_industry_code_set Nullable(String),
    description_original Nullable(String),
    description_language Nullable(String),
    description_en Nullable(String),
    description_translated_at Nullable(DateTime64(3, 'UTC')),
    description_translation_provider Nullable(String),
    description_translation_model Nullable(String),
    nace_revision Nullable(String),
    nace_code Nullable(String),
    nace_normalized_code Nullable(String),
    nace_mapping_method LowCardinality(String),
    nace_mapping_status LowCardinality(String),
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (business_id, source_record_id);
