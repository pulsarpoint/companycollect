CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.dk_cvr_company_detail_failures
(
    cvr                  String,
    http_status          UInt16,
    first_failed_at      DateTime64(3, 'UTC'),
    failed_at            DateTime64(3, 'UTC'),
    failure_count        UInt32,
    decision             LowCardinality(String),
    source_asset         LowCardinality(String),
    source_partition_key String,
    source_url           String,
    source_run_id        String,
    failure_object_key   String
)
ENGINE = MergeTree
ORDER BY (cvr, http_status, failed_at, source_run_id);
