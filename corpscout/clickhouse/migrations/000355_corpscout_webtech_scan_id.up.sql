CREATE DATABASE IF NOT EXISTS corpscout;

-- Stable remote scan identity complements the Dagster run that indexed it.
ALTER TABLE corpscout.webtech_domain_scan_results
    ADD COLUMN IF NOT EXISTS scan_id String AFTER partition_key;
