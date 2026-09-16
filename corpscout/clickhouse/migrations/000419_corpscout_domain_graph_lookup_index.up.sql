CREATE DATABASE IF NOT EXISTS corpscout;

-- Neighbor IDs are scattered across the graph. The old 8,192-row granules
-- read millions of domain names for a few thousand neighbors.
ALTER TABLE corpscout.commoncrawl_domain_graph_nodes
ADD PROJECTION IF NOT EXISTS by_node_id_lookup
(
    SELECT graph_release, node_id, root_domain, n_hosts
    ORDER BY (graph_release, node_id)
)
WITH SETTINGS (index_granularity = 128);

-- Keep the old access path available until the new projection is complete.
ALTER TABLE corpscout.commoncrawl_domain_graph_nodes
MATERIALIZE PROJECTION by_node_id_lookup
SETTINGS mutations_sync = 1;

ALTER TABLE corpscout.commoncrawl_domain_graph_nodes
DROP PROJECTION IF EXISTS by_node_id;
