CREATE DATABASE IF NOT EXISTS corpscout;

-- Registrations classified registry_level or unallocated (rdap_network_registry_class, migration
-- 000450) answer only the IP that was queried. The trie source excludes their segments, so the
-- poisoned entries stop being served and a reclassification after a reference refresh takes
-- effect at the next dictionary reload without a code change. The exclusion sits in a subquery
-- because ClickHouse resolves the argMax alias network_key inside an outer WHERE.
GRANT SELECT ON corpscout.rdap_network_registry_class TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.rdap_network_registry_class_current TO corpscout_rdap_dictionary;

DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;
DROP VIEW IF EXISTS corpscout.rdap_network_segments_current;

CREATE VIEW corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM
(
    SELECT network_key, cidr, derived_at
    FROM corpscout.rdap_network_segments FINAL
    WHERE segment_role = 'lookup_result'
      AND prefix_length > 0
      AND network_key NOT IN
      (
          SELECT network_key
          FROM corpscout.rdap_network_registry_class_current
          WHERE registry_class != 'reusable'
      )
)
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
