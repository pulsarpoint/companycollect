ALTER TABLE corpscout.commoncrawl_domain_graph_nodes
ADD PROJECTION IF NOT EXISTS by_node_id
(
    SELECT * ORDER BY (graph_release, node_id)
);

ALTER TABLE corpscout.commoncrawl_domain_graph_nodes
MATERIALIZE PROJECTION by_node_id
SETTINGS mutations_sync = 1;

ALTER TABLE corpscout.commoncrawl_domain_graph_nodes
DROP PROJECTION IF EXISTS by_node_id_lookup;
