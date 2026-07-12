DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;

REVOKE SELECT ON corpscout.rdap_network_segments_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.rdap_network_segments FROM corpscout_rdap_dictionary;
DROP USER IF EXISTS corpscout_rdap_dictionary;

CREATE DICTIONARY IF NOT EXISTS corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(CLICKHOUSE(DB 'corpscout' TABLE 'rdap_network_segments_current'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);
