CREATE DATABASE IF NOT EXISTS corpscout;

-- Only validated, complete release partitions may be published from staging.
-- Keep the legacy signals table readable until the importer and consumers switch.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_graph_ranks
(
    graph_release LowCardinality(String),
    root_domain String CODEC(ZSTD(1)),
    cc_harmonic_centrality Float64,
    cc_harmonic_rank UInt64,
    cc_pagerank Float64,
    cc_pagerank_rank UInt64,
    n_hosts Nullable(UInt32),
    source_run_id String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY graph_release
ORDER BY (root_domain, graph_release);
