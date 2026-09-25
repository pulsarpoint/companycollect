CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.rdap_network_registry_class_derived;
DROP VIEW IF EXISTS corpscout.rdap_network_registry_class_current;
DROP TABLE IF EXISTS corpscout.rdap_network_registry_class;
DROP VIEW IF EXISTS corpscout.ip_registry_ready;
DROP DICTIONARY IF EXISTS corpscout.ip_registry_special_trie;

REVOKE SELECT ON corpscout.ip_registry_special_trie_source FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_special_segments_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_special_segments FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_current_snapshots FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.ip_registry_snapshots FROM corpscout_rdap_dictionary;

DROP VIEW IF EXISTS corpscout.ip_registry_special_trie_source;
DROP VIEW IF EXISTS corpscout.ip_registry_iana_blocks_rule_current;
DROP VIEW IF EXISTS corpscout.ip_registry_holder_blocks_current;
DROP VIEW IF EXISTS corpscout.ip_registry_special_segments_current;
DROP VIEW IF EXISTS corpscout.ip_registry_iana_blocks_current;
DROP TABLE IF EXISTS corpscout.ip_registry_holder_blocks;
DROP TABLE IF EXISTS corpscout.ip_registry_special_segments;
DROP TABLE IF EXISTS corpscout.ip_registry_iana_blocks;
DROP VIEW IF EXISTS corpscout.ip_registry_current_snapshots;
DROP TABLE IF EXISTS corpscout.ip_registry_snapshots;
