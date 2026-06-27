CREATE DATABASE IF NOT EXISTS corpscout;

-- Industry classification of a domain from the embedding/NACE pass, split out of commoncrawl_domains
-- so that table can become the thin domain master (written by every pass). One row per (domain,
-- crawl), written by the industry pass only, keyed to commoncrawl_domains via root_domain. This
-- migration only creates the table. Backfilling the existing rows and slimming commoncrawl_domains
-- are separate, later steps.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_industries
(
    crawl_id LowCardinality(String),
    root_domain String,
    source_url String,
    emails Array(String),
    email_count UInt32,
    page_type LowCardinality(String),
    page_type_score Float32,
    nace_code String,
    nace_label String,
    nace_division LowCardinality(String),
    nace_confident UInt8,
    nace_confidence Float32,
    nace_margin Float32,
    nace_score Float32,
    nace_method LowCardinality(String),
    nace_top3_codes Array(String),
    nace_top3_labels Array(String),
    nace_top3_scores Array(Float32),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
