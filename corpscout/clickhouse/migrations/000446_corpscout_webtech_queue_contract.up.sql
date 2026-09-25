CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract: one partition per task so cleanup is DROP PARTITION,
-- sorted by task for reads, and a submission_id so a retried import replaces only
-- its own rows. Queue inputs are purged after completion, so the table is rebuilt
-- only while empty.
SELECT throwIf(count() > 0, 'webtech_scan_input must be empty before its layout changes')
FROM corpscout.webtech_scan_input;

DROP TABLE IF EXISTS corpscout.webtech_scan_input;

CREATE TABLE corpscout.webtech_scan_input
(
    task_id String,
    input_id String,
    root_domain String,
    website_origin String,
    page_url String,
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND length(input_id) = 64,
    CONSTRAINT valid_target CHECK notEmpty(root_domain)
        AND protocol(page_url) IN ('http', 'https') AND notEmpty(domain(page_url)),
    CONSTRAINT valid_source CHECK notEmpty(source_name) AND notEmpty(submission_id)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, input_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
