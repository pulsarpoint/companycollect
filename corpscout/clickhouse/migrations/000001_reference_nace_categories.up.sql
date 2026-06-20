CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.nace_categories
(
    classification_version LowCardinality(String),
    code String,
    normalized_code String,
    parent_code Nullable(String),
    level LowCardinality(String),
    section_code Nullable(String),
    description_en String,
    concept_uri String,
    parent_concept_uri Nullable(String),
    source_scheme_uri String,
    source_url String,
    source_payload_hash FixedString(64),
    valid_from Date,
    valid_to Nullable(Date),
    is_current UInt8,
    source_run_id String,
    pulled_at DateTime64(3, 'UTC'),
    _dlt_load_id String,
    _dlt_id String
)
ENGINE = ReplacingMergeTree(pulled_at)
ORDER BY (classification_version, normalized_code);
