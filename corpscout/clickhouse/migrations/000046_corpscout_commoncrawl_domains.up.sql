CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domains
(
    crawl_id LowCardinality(String),
    url String,
    root_domain String,
    subdomain String,
    emails Array(String),
    email_count UInt32,
    page_type LowCardinality(String),
    page_type_score Float32,
    nace_code String,
    nace_label String,
    nace_division LowCardinality(String),
    nace_confident UInt8,
    nace_margin Float32,
    nace_score Float32,
    nace_method LowCardinality(String),
    nace_top3_codes Array(String),
    nace_top3_labels Array(String),
    nace_top3_scores Array(Float32),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, url, crawl_id);
