CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_graph_nodes
(
    graph_release LowCardinality(String),
    node_id UInt32 CODEC(Delta, ZSTD(1)),
    root_domain String CODEC(ZSTD(1)),
    n_hosts UInt32 CODEC(ZSTD(1)),
    source_etag LowCardinality(String),
    source_run_id String,
    PROJECTION by_node_id
    (
        SELECT * ORDER BY (graph_release, node_id)
    )
)
ENGINE = MergeTree
PARTITION BY graph_release
ORDER BY (graph_release, root_domain);

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_graph_edges
(
    graph_release LowCardinality(String),
    source_node_id UInt32 CODEC(Delta, ZSTD(1)),
    target_node_id UInt32 CODEC(Delta, ZSTD(1)),
    source_etag LowCardinality(String),
    source_run_id String,
    PROJECTION by_target
    (
        SELECT * ORDER BY (graph_release, target_node_id, source_node_id)
    )
)
ENGINE = MergeTree
PARTITION BY graph_release
ORDER BY (graph_release, source_node_id, target_node_id);

-- Only complete, validated releases appear here. Graph IDs are release-local.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_graph_snapshots
(
    graph_release String,
    node_count UInt64,
    edge_count UInt64,
    loop_count UInt64,
    vertices_url String,
    edges_url String,
    vertices_etag String,
    edges_etag String,
    source_index_url String,
    source_run_id String,
    published_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(published_at)
ORDER BY graph_release;

-- Restrict both adjacency scans before resolving numeric IDs to domain names.
CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_domain_connections AS
WITH seeds AS
(
    SELECT node_id FROM corpscout.commoncrawl_domain_graph_nodes
    WHERE graph_release = {graph_release:String} AND root_domain = {domain:String}
      AND graph_release IN (SELECT graph_release FROM corpscout.commoncrawl_domain_graph_snapshots FINAL)
), neighbors AS
(
    SELECT target_node_id AS node_id, toUInt8(1) AS outgoing, toUInt8(0) AS incoming
    FROM corpscout.commoncrawl_domain_graph_edges
    WHERE graph_release = {graph_release:String} AND source_node_id IN (SELECT node_id FROM seeds)
    UNION ALL
    SELECT source_node_id AS node_id, toUInt8(0) AS outgoing, toUInt8(1) AS incoming
    FROM corpscout.commoncrawl_domain_graph_edges
    WHERE graph_release = {graph_release:String} AND target_node_id IN (SELECT node_id FROM seeds)
), connections AS
(
    SELECT node_id, max(outgoing) AS outgoing, max(incoming) AS incoming
    FROM neighbors GROUP BY node_id
)
SELECT n.root_domain AS connected_domain, c.outgoing, c.incoming,
    toUInt8(c.outgoing AND c.incoming) AS reciprocal, n.n_hosts
FROM
(
    SELECT node_id, root_domain, n_hosts FROM corpscout.commoncrawl_domain_graph_nodes
    WHERE graph_release = {graph_release:String}
      AND node_id IN (SELECT node_id FROM connections)
) AS n INNER JOIN connections AS c USING (node_id)
WHERE n.root_domain != {domain:String};
