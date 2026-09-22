CREATE DATABASE IF NOT EXISTS corpscout;

-- Immutable submission receipts let a later run reconnect after a worker interruption.
-- PostgreSQL advisory locks and the Dagster pool serialize processors. These are
-- audit/recovery records, not a ClickHouse claim queue.
CREATE TABLE IF NOT EXISTS corpscout.website_crawl_submissions (
    crawl_type Enum8('full'=1, 'jobs'=2, 'site_info'=3),
    domain String,
    request_id String,
    input_revision UInt64,
    work_key String,
    run_id String,
    request_json String,
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_request CHECK isValidJSON(request_json)
)
ENGINE = ReplacingMergeTree
ORDER BY (crawl_type, domain, request_id);

CREATE TABLE IF NOT EXISTS corpscout.website_full_crawl_results (
    domain String,
    website_url String,
    request_id String,
    attempt UInt32,
    input_revision UInt64,
    work_key String,
    run_id String,
    state Enum8('completed'=1, 'failed'=2, 'cancelled'=3),
    crawl_status LowCardinality(String),
    successful Bool,
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6),
    site_info Nullable(String),
    page_observations String DEFAULT '[]',
    pages String DEFAULT '[]',
    model_usage String DEFAULT '{}',
    error String,
    s3_path String,
    s3_state LowCardinality(String),
    CONSTRAINT success_is_completed CHECK NOT successful OR (state='completed' AND empty(error)),
    CONSTRAINT valid_site_info CHECK isNull(site_info) OR isValidJSON(site_info),
    CONSTRAINT valid_sections CHECK isValidJSON(page_observations) AND isValidJSON(pages) AND isValidJSON(model_usage)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (domain, request_id, attempt);

-- Newest attempt per domain and effective crawl configuration, including failures.
CREATE VIEW IF NOT EXISTS corpscout.website_full_crawl_results_latest AS
SELECT * FROM corpscout.website_full_crawl_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

-- An error never hides a previous valid success for the same effective request.
CREATE VIEW IF NOT EXISTS corpscout.website_full_crawl_results_latest_success AS
SELECT * FROM corpscout.website_full_crawl_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE TABLE IF NOT EXISTS corpscout.website_jobs_crawl_results (
    domain String,
    website_url String,
    request_id String,
    attempt UInt32,
    input_revision UInt64,
    work_key String,
    run_id String,
    state Enum8('completed'=1, 'failed'=2, 'cancelled'=3),
    crawl_status LowCardinality(String),
    successful Bool,
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6),
    site_info Nullable(String),
    page_observations String DEFAULT '[]',
    pages String DEFAULT '[]',
    model_usage String DEFAULT '{}',
    error String,
    s3_path String,
    s3_state LowCardinality(String),
    CONSTRAINT success_is_completed CHECK NOT successful OR (state='completed' AND empty(error)),
    CONSTRAINT valid_site_info CHECK isNull(site_info) OR isValidJSON(site_info),
    CONSTRAINT valid_sections CHECK isValidJSON(page_observations) AND isValidJSON(pages) AND isValidJSON(model_usage)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (domain, request_id, attempt);

-- Newest attempt per domain and effective crawl configuration, including failures.
CREATE VIEW IF NOT EXISTS corpscout.website_jobs_crawl_results_latest AS
SELECT * FROM corpscout.website_jobs_crawl_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

-- An error never hides a previous valid success for the same effective request.
CREATE VIEW IF NOT EXISTS corpscout.website_jobs_crawl_results_latest_success AS
SELECT * FROM corpscout.website_jobs_crawl_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE TABLE IF NOT EXISTS corpscout.website_site_info_results (
    domain String,
    website_url String,
    request_id String,
    attempt UInt32,
    input_revision UInt64,
    work_key String,
    run_id String,
    state Enum8('completed'=1, 'failed'=2, 'cancelled'=3),
    crawl_status LowCardinality(String),
    successful Bool,
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6),
    site_info Nullable(String),
    page_observations String DEFAULT '[]',
    pages String DEFAULT '[]',
    model_usage String DEFAULT '{}',
    error String,
    s3_path String,
    s3_state LowCardinality(String),
    CONSTRAINT success_is_completed CHECK NOT successful OR (state='completed' AND empty(error)),
    CONSTRAINT valid_site_info CHECK isNull(site_info) OR isValidJSON(site_info),
    CONSTRAINT valid_sections CHECK isValidJSON(page_observations) AND isValidJSON(pages) AND isValidJSON(model_usage)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (domain, request_id, attempt);

-- Newest attempt per domain and effective crawl configuration, including failures.
CREATE VIEW IF NOT EXISTS corpscout.website_site_info_results_latest AS
SELECT * FROM corpscout.website_site_info_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

-- An error never hides a previous valid success for the same effective request.
CREATE VIEW IF NOT EXISTS corpscout.website_site_info_results_latest_success AS
SELECT * FROM corpscout.website_site_info_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;
