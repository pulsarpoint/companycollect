CREATE DATABASE IF NOT EXISTS corpscout;

-- Terminal Ratsit crawl outcomes. Temporal owns pending and running work, while
-- this table records the durable result used to decide when a company is due
-- for another crawl. Raw response content and its hash remain in S3.
CREATE TABLE IF NOT EXISTS corpscout.se_company_ratsit_crawl_results
(
    company_id String,
    batch_id UUID,
    outcome LowCardinality(String),
    selected_at DateTime64(3, 'UTC'),
    attempted_at DateTime64(3, 'UTC'),
    completed_at DateTime64(3, 'UTC'),
    http_status Nullable(UInt16),
    source_url String,
    source_bucket LowCardinality(String),
    source_object_key String,
    content_size_bytes UInt64,
    duration_ms UInt64,
    attempt_count UInt16,
    error_type LowCardinality(String),
    error_message String CODEC(ZSTD(3)),
    temporal_workflow_id String,
    temporal_run_id String,
    recorded_at DateTime64(3, 'UTC'),

    CONSTRAINT se_company_ratsit_company_id CHECK match(company_id, '^[0-9]{10}$'),
    CONSTRAINT se_company_ratsit_outcome CHECK outcome IN
        ('success', 'not_found', 'retry_exhausted', 'blocked', 'selector_changed'),
    CONSTRAINT se_company_ratsit_timestamps CHECK
        selected_at <= attempted_at
        AND attempted_at <= completed_at
        AND completed_at <= recorded_at,
    CONSTRAINT se_company_ratsit_attempt_count CHECK attempt_count > 0,
    CONSTRAINT se_company_ratsit_source_url CHECK
        source_url = concat('https://www.ratsit.se/', company_id),
    CONSTRAINT se_company_ratsit_object_location CHECK
        source_object_key = '' OR source_bucket != '',
    CONSTRAINT se_company_ratsit_content_location CHECK
        content_size_bytes = 0 OR source_object_key != '',
    CONSTRAINT se_company_ratsit_success CHECK
        outcome != 'success'
        OR (
            ifNull(http_status >= 200 AND http_status < 300, false)
            AND source_object_key != ''
            AND content_size_bytes > 0
        )
)
ENGINE = ReplacingMergeTree(recorded_at)
PARTITION BY toYYYYMM(completed_at)
ORDER BY (company_id, batch_id);

-- Retry inserts collapse by (company_id, batch_id) in the table. This view also
-- reduces all completed batches to the newest effective state for selection and
-- freshness checks without relying on background merge timing.
CREATE VIEW IF NOT EXISTS corpscout.se_company_ratsit_current AS
SELECT
    company_id,
    latest.1 AS latest_batch_id,
    latest.2 AS latest_outcome,
    latest.3 AS latest_selected_at,
    latest.4 AS latest_attempted_at,
    latest.5 AS latest_completed_at,
    latest.6 AS latest_http_status,
    latest.7 AS latest_source_url,
    latest.8 AS latest_source_bucket,
    latest.9 AS latest_source_object_key,
    latest.10 AS latest_content_size_bytes,
    latest.11 AS latest_duration_ms,
    latest.12 AS latest_attempt_count,
    latest.13 AS latest_error_type,
    latest.14 AS latest_error_message,
    latest.15 AS latest_temporal_workflow_id,
    latest.16 AS latest_temporal_run_id,
    latest.17 AS latest_recorded_at,
    if(
        successful_crawl_count = 0,
        NULL,
        latest_success_at_value
    ) AS latest_success_at
FROM
(
    SELECT
        company_id,
        argMax(
            tuple(
                batch_id,
                outcome,
                selected_at,
                attempted_at,
                completed_at,
                http_status,
                source_url,
                source_bucket,
                source_object_key,
                content_size_bytes,
                duration_ms,
                attempt_count,
                error_type,
                error_message,
                temporal_workflow_id,
                temporal_run_id,
                recorded_at
            ),
            tuple(completed_at, recorded_at, batch_id)
        ) AS latest,
        countIf(outcome = 'success') AS successful_crawl_count,
        maxIf(completed_at, outcome = 'success') AS latest_success_at_value
    FROM corpscout.se_company_ratsit_crawl_results
    GROUP BY company_id
);
