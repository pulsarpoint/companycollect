CREATE DATABASE IF NOT EXISTS corpscout;

-- Repurpose the orphaned commoncrawl_page_signals (built for page contacts, never written) as the
-- per-domain page/decision signals the industry pass fills: the page classification plus the NACE
-- ranking quality. One row per domain. Contacts live in commoncrawl_company_profile, so the old
-- contact and social-link columns are dropped. Empty table, so drop and recreate is lossless.
DROP TABLE IF EXISTS corpscout.commoncrawl_page_signals;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_signals
(
    crawl_id LowCardinality(String),
    root_domain String,
    subdomain String,
    source_url String,
    page_type LowCardinality(String),
    page_type_score Float32,
    nace_confident UInt8,
    nace_margin Float32,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
