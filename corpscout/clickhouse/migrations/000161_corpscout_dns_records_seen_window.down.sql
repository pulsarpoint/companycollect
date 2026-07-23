CREATE DATABASE IF NOT EXISTS corpscout;

-- Detach the dual-write trigger before removing its target so a concurrent ingest insert fails
-- (and the outboxes retry) rather than writing into a dropped table.
DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_v2_mv;

DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records_v2;
