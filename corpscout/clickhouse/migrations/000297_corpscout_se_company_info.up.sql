CREATE DATABASE IF NOT EXISTS corpscout;

-- Sweden company information (2026-08). Basic-info slice 4 (2026-09-08) dropped the five
-- se_company_info* tables by hand and their DDL left this file, and the observation cache
-- below stays: every row is a paid model answer the basic-info LLM extractor reuses.
CREATE TABLE IF NOT EXISTS corpscout.se_company_info_enrichment_observation
(
    suggestion_id UUID,
    company_id String,
    input_hash FixedString(64),
    suggestion String,
    raw_response String,
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT valid_suggestion CHECK isValidJSON(suggestion)
)
ENGINE = MergeTree
ORDER BY (company_id, input_hash, created_at);
