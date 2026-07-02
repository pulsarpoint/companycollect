CREATE DATABASE IF NOT EXISTS corpscout;

-- Per-domain HTML-head content signals from the tech pass, captured from the domain's primary page: title,
-- the FULL meta map (description, keywords, generator, robots, viewport, og and twitter cards, ...),
-- canonical, hreflang alternates, every JSON-LD @type seen, and charset. Captured broadly so nothing is
-- skipped -- downstream classification (hiring via JobPosting, ecommerce via Product, language, ...) is
-- derived later. One row per (domain, crawl). ReplacingMergeTree on resolved_at -> read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_page_meta
(
    crawl_id LowCardinality(String),
    root_domain String,
    source_url String,
    title String,
    meta Map(LowCardinality(String), String),
    canonical String,
    hreflang Array(LowCardinality(String)),
    jsonld_types Array(LowCardinality(String)),
    charset LowCardinality(String),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
