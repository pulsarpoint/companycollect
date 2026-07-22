-- The DNS writers have moved to commoncrawl_domain_dns_record_ingest, all legacy buckets have
-- been backfilled, and the original wide source is no longer advancing. Remove its synchronous
-- triggers before dropping the source table so only the v2 ingest triggers remain active.
--
-- This migration permanently deletes the legacy observation history. Apply it only after a
-- successful backup and validation of the normalized record and sighting tables.
CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_mv;
DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_mv;
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_observations
SETTINGS max_table_size_to_drop = 150000000000;
