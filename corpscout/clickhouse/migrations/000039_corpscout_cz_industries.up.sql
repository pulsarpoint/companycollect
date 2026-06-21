CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.cz_industries
(
    ico String,
    source_industry_code String,
    source_industry_code_set LowCardinality(String),
    description_original Nullable(String),
    description_language LowCardinality(String),
    description_en Nullable(String),
    description_translated_at Nullable(DateTime64(3, 'UTC')),
    description_translation_provider Nullable(String),
    description_translation_model Nullable(String),
    nace_revision LowCardinality(String),
    nace_code String,
    nace_normalized_code String,
    nace_mapping_method LowCardinality(String),
    nace_mapping_status LowCardinality(String),
    is_primary UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (ico, source_industry_code);
