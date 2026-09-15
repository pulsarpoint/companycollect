CREATE DATABASE IF NOT EXISTS corpscout;

-- Keep migration 409's historical S3 index intact. New task outcomes carry the full answer.
CREATE TABLE corpscout.company_brave_info
(
    result_id String,
    task_id String,
    input_id String,
    work_key String,
    attempt UInt32,
    status LowCardinality(String),
    export_batch_id String,
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    query String,
    query_type LowCardinality(String),
    processor_version LowCardinality(String),
    answer_text String,
    route LowCardinality(String),
    source_url String,
    error_type LowCardinality(String),
    source_run_id String,
    completed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (task_id, result_id);

-- Correct immediately after a replay, before background merges have happened.
CREATE VIEW corpscout.company_brave_info_deduplicated AS
SELECT * FROM corpscout.company_brave_info FINAL;

CREATE VIEW corpscout.se_company_brave_input AS
SELECT company_id AS input_id, company_id,
    trimBoth(ifNull(legal_name, '')) AS company_name, 'SE' AS country_code
FROM corpscout.se_company_basic_info FINAL
WHERE status='active' AND trimBoth(ifNull(legal_name, '')) != '';
