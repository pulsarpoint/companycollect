CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.text_translations
(
    source_slug       LowCardinality(String),
    field             LowCardinality(String),
    source_text_hash  UInt64,
    source_lang       LowCardinality(String),
    target_lang       LowCardinality(String),
    translated_text   String,
    provider          LowCardinality(String),
    model             LowCardinality(String),
    version           UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_slug, field, source_text_hash);
