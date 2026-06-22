CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.nace_category_embeddings
(
    code String,
    level LowCardinality(String),
    section_code LowCardinality(String),
    parent_code String,
    division LowCardinality(String),
    label String,
    embedding_text String,
    embedding Array(Float32),
    embedding_dim UInt32,
    embedding_model LowCardinality(String),
    embedding_variant LowCardinality(String),
    classification_version LowCardinality(String),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (code);
