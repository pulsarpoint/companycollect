CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.company_brave_search_results
(
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    query String,
    prompt_version LowCardinality(String),
    status LowCardinality(String),
    connection_mode LowCardinality(String),
    proxy_name LowCardinality(String),
    source_url String,
    fetched_at DateTime64(3, 'UTC'),
    answer_bucket String,
    answer_object_key String,
    answer_bytes UInt64,
    error_type LowCardinality(String),
    source_run_id String
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (country_code, company_id, source_run_id);
