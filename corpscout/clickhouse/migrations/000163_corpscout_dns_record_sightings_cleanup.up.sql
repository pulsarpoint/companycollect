CREATE DATABASE IF NOT EXISTS corpscout;

-- The seen-window records table has been cut over (000162), both DNS writers are flushing through
-- the v2 trigger, and the normalized data has soaked for at least one full scan cycle. Remove the
-- superseded copies: the pre-cutover record dimension and the per-scan sighting fact.
--
-- This migration permanently deletes the per-sighting history (scan_id, per-observation ttl,
-- name_server, rcode). Apply it only after a successful backup. The legacy records table (~80 GiB)
-- exceeds ClickHouse's default 50 GB deletion guard, so the guard is raised for these reviewed
-- statements only.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records_legacy
SETTINGS max_table_size_to_drop = 150000000000;

DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_sightings
SETTINGS max_table_size_to_drop = 150000000000;
