CREATE DATABASE IF NOT EXISTS corpscout;

-- Graph-derived authority signals per domain, from the CommonCrawl DOMAIN-level web graph (the
-- *-domain-ranks.txt release). A SEPARATE source from the page-fetch worker, ingested by its own process on
-- its own cadence — so it is a SATELLITE table joined on root_domain, NOT a column on commoncrawl_domains
-- (a different ReplacingMergeTree whole-row writer would clobber the worker's columns). Mirrors how Open
-- PageRank already lives in its own table (open_page_rank_domains).
-- Columns map 1:1 to the ranks file (domain un-reversed: host_rev "com.example" -> root_domain "example.com"):
--   harmonicc_val -> cc_harmonic_centrality (shortest-path reachability, the spam-resistant complement to PageRank)
--   harmonicc_pos -> cc_harmonic_rank      (global rank, 1 = most central)
--   pr_val        -> cc_pagerank           (PageRank over CommonCrawl, mostly redundant with open_page_rank)
--   pr_pos        -> cc_pagerank_rank
--   n_hosts       -> n_hosts               (hosts aggregated under this domain)
-- One row per (domain, crawl). ReplacingMergeTree on resolved_at -> read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_graph_signals
(
    crawl_id LowCardinality(String),
    root_domain String,
    cc_harmonic_centrality Float64,
    cc_harmonic_rank UInt32,
    cc_pagerank Float64,
    cc_pagerank_rank UInt32,
    n_hosts UInt32,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
