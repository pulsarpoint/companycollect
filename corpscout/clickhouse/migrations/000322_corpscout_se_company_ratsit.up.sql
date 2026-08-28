CREATE DATABASE IF NOT EXISTS corpscout;

-- Terminal Ratsit crawl outcomes. Temporal owns pending and running work, while
-- this table records the durable result used to decide when a company is due
-- for another crawl. Raw response content and its hash remain in S3.
CREATE TABLE IF NOT EXISTS corpscout.se_company_ratsit
(
    company_id String,
    batch_id UUID,
    outcome LowCardinality(String),
    failure_type LowCardinality(String),
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
    CONSTRAINT se_company_ratsit_outcome CHECK outcome IN ('success', 'failure'),
    CONSTRAINT se_company_ratsit_failure_type CHECK
        (outcome = 'success' AND failure_type = '')
        OR (outcome = 'failure' AND failure_type != ''),
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
