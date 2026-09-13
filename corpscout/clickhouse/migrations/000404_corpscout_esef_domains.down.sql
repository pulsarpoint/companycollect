CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000404. The extractor's asset (esef_domains_clickhouse) and sensor must be undeployed
-- with it, or the next sensor tick fails on the missing table.
DROP VIEW IF EXISTS corpscout.se_esef_domains;
DROP TABLE IF EXISTS corpscout.esef_domains;
