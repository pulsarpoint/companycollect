CREATE DATABASE IF NOT EXISTS corpscout;

-- Per-domain HTTP security posture from the CommonCrawl tech pass: the FULL response-header map of the
-- domain's primary page, captured verbatim so no header is ever skipped. All analysis (hygiene score,
-- server/x-powered-by version disclosure, cookie flags, issue-template mapping) is DERIVED from this map
-- later in SQL, not at ingest -- capture broadly now, analyze later. Header maps are tiny and compress
-- hard. One row per (domain, crawl). ReplacingMergeTree on resolved_at -> read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_security
(
    crawl_id LowCardinality(String),
    root_domain String,
    source_url String,
    headers Map(LowCardinality(String), String),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
