DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;
DROP VIEW IF EXISTS corpscout.rdap_network_segments_current;

CREATE VIEW corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM corpscout.rdap_network_segments FINAL
WHERE segment_role = 'lookup_result'
GROUP BY cidr;

CREATE DICTIONARY corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'rdap_network_segments_current'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);
