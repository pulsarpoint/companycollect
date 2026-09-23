CREATE DATABASE IF NOT EXISTS corpscout;

-- Rollback removes only the new objects, in dependency order.
DROP VIEW IF EXISTS corpscout.ip_enrichment_current;
DROP TABLE IF EXISTS corpscout.ip_enrichment_results;
DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

