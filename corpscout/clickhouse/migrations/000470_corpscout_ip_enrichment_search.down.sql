CREATE DATABASE IF NOT EXISTS corpscout;

-- The view owns its table: dropping it drops the search table. Nothing else writes it.
DROP VIEW IF EXISTS corpscout.ip_enrichment_search;
