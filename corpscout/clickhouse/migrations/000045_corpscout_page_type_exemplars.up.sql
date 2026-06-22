CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.page_type_exemplars
(
    page_type LowCardinality(String),
    root_domain String,
    source_url String,
    signal_source LowCardinality(String),
    text String,
    embedding Array(Float32),
    embedding_dim UInt32,
    embedding_model LowCardinality(String),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (page_type, root_domain);
