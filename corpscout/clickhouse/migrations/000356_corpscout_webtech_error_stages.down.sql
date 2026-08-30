CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.webtech_domain_scan_results
    DROP COLUMN IF EXISTS extension_failure_stage;

ALTER TABLE corpscout.webtech_domain_scan_results
    DROP COLUMN IF EXISTS timeout_stage;
