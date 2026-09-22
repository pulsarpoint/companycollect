CREATE DATABASE IF NOT EXISTS corpscout;

-- Completed attempts only. A retry of a write must keep the entire row unchanged.
-- There is deliberately no TTL: rescan_old=false must remember old searches.
CREATE TABLE corpscout.company_brave_search_results
(
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    query_type LowCardinality(String),
    query String,
    result_id UUID,
    status Enum8('success' = 1, 'error' = 2),
    answer_text String,
    completed_at DateTime64(6, 'UTC'),
    error_type LowCardinality(String),
    error_stage LowCardinality(String),
    route LowCardinality(String),
    source_url String,
    elapsed_ms UInt32,
    answer_timeout_ms UInt32,
    challenge_runs_json String DEFAULT '[]',
    task_id UUID,
    execution_id UUID,
    source_run_id String,
    input_id String,
    attempt UInt32,
    processor_version LowCardinality(String),
    CONSTRAINT valid_search CHECK notEmpty(country_code) AND notEmpty(company_id) AND notEmpty(query_type),
    CONSTRAINT nonempty_success CHECK status != 'success' OR notEmpty(trimBoth(answer_text))
)
ENGINE = ReplacingMergeTree
ORDER BY (country_code, company_id, query_type, result_id);

-- Full newest attempt for each company/query type, including errors.
CREATE VIEW corpscout.company_brave_search_results_latest AS
SELECT * FROM corpscout.company_brave_search_results FINAL
ORDER BY completed_at DESC, result_id DESC
LIMIT 1 BY country_code, company_id, query_type;

-- The downstream contract remains latest successful answers, even if a rescan fails.
CREATE MATERIALIZED VIEW corpscout.se_company_brave_search_successes
TO corpscout.se_company_brave_search_results_latest_success AS
SELECT toString(result_id) AS result_id, toString(task_id) AS task_id,
    input_id, '' AS work_key, attempt, toString(status) AS status,
    '' AS export_batch_id, country_code, company_id, company_name, query,
    query_type, processor_version, answer_text, route, source_url, error_type,
    source_run_id, completed_at, '' AS archive_path
FROM corpscout.company_brave_search_results
WHERE country_code = 'SE' AND status = 'success';
