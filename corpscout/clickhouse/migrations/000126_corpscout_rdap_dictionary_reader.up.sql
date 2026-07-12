CREATE DATABASE IF NOT EXISTS corpscout;

DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;

CREATE USER IF NOT EXISTS corpscout_rdap_dictionary
HOST LOCAL
IDENTIFIED WITH no_password;

GRANT SELECT ON corpscout.rdap_network_segments TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.rdap_network_segments_current TO corpscout_rdap_dictionary;

CREATE DICTIONARY IF NOT EXISTS corpscout.rdap_network_trie
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
