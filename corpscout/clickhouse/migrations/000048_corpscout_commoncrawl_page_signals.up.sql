CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_signals
(
    crawl_id LowCardinality(String),
    url String,
    root_domain String,
    subdomain String,
    emails Array(String),
    social_platforms Array(String),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, url, crawl_id);
