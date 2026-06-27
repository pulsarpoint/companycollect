CREATE DATABASE IF NOT EXISTS corpscout;

-- A domain can legitimately have MULTIPLE industries, so commoncrawl_industries is one row per
-- (domain, nace_code). rank orders them by score, is_primary marks the headline industry, score is
-- the cosine match. Written by the industry pass only, keyed to commoncrawl_domains via root_domain.
-- Runner-up candidates are simply lower-rank rows (this replaces the single nace_code + top3 arrays).
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_industries
(
    crawl_id LowCardinality(String),
    root_domain String,
    nace_code String,
    nace_label String,
    nace_division LowCardinality(String),
    rank UInt8,
    is_primary UInt8,
    score Float32,
    nace_method LowCardinality(String),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id, nace_code);
