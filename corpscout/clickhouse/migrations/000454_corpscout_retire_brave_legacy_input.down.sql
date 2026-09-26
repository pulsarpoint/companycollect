CREATE DATABASE IF NOT EXISTS corpscout;

-- Schema rollback only. Removed queue inputs cannot be restored.
CREATE TABLE IF NOT EXISTS corpscout.company_brave_search_input
(
    input_id String,
    company_id String,
    company_name String,
    country_code LowCardinality(String),
    task_id String,
    INDEX task_id_index task_id TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
PRIMARY KEY input_id
ORDER BY (input_id, task_id);
