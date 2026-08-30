CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.webtech_domain_scan_results
    DROP COLUMN IF EXISTS scan_id;
