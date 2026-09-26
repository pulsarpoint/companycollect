CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE corpscout.company_brave_queue_input
(
    task_id String,
    input_id String,
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND country_code = 'SE'
        AND notEmpty(company_id) AND input_id = concat(country_code, ':', company_id),
    CONSTRAINT valid_name CHECK notEmpty(trimBoth(company_name)),
    CONSTRAINT valid_source CHECK notEmpty(source_name) AND notEmpty(submission_id)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, input_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;

-- Retain every selected company, including freshness skips, before input cleanup.
CREATE TABLE corpscout.company_brave_task_sources
(
    task_id String,
    input_id String,
    country_code LowCardinality(String),
    company_id String,
    company_name String,
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    submitted_at DateTime64(6, 'UTC'),
    recorded_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (task_id, input_id);
