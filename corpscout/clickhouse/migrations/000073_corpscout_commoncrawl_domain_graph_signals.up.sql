CREATE DATABASE IF NOT EXISTS corpscout;

-- Graph-derived authority signals per domain, from the CommonCrawl web graph. This is a SEPARATE source
-- from the page-fetch worker, ingested by its own process on its own refresh cadence — so it is a SATELLITE
-- table joined on root_domain, NOT a column on commoncrawl_domains (a ReplacingMergeTree whole-row writer
-- from a different process would clobber the worker's columns). Mirrors how Open PageRank already lives in
-- its own table (open_page_rank_domains).
--   - cc_harmonic_centrality: shortest-path reachability — the spam-resistant complement to PageRank
--     (a link farm can't fabricate real short paths from the established web).
--   - cc_pagerank: optional, mostly redundant with open_page_rank (which is itself PageRank over CommonCrawl).
-- One row per (domain, crawl). ReplacingMergeTree on resolved_at -> read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_graph_signals
(
    crawl_id LowCardinality(String),
    root_domain String,
    cc_harmonic_centrality Float64,
    cc_pagerank Nullable(Float64),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
