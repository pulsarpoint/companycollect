CREATE DATABASE IF NOT EXISTS corpscout;

-- Retain source membership after completed draft partitions are removed, including
-- inputs skipped due to freshness. This stores URLs/provenance, never payloads.
CREATE TABLE IF NOT EXISTS corpscout.queue_task_sources
(
    task_type LowCardinality(String),
    task_id String,
    domain String,
    website_url String,
    source_name LowCardinality(String),
    recorded_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_type CHECK task_type IN ('webtech', 'full', 'jobs', 'site_info')
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (task_type, task_id, domain, website_url, source_name);
