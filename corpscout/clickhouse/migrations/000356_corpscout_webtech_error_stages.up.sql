CREATE DATABASE IF NOT EXISTS corpscout;

-- Queryable failure diagnostics complement the complete result retained in RustFS.
ALTER TABLE corpscout.webtech_domain_scan_results
    ADD COLUMN IF NOT EXISTS timeout_stage LowCardinality(String) DEFAULT '' AFTER outcome;

ALTER TABLE corpscout.webtech_domain_scan_results
    ADD COLUMN IF NOT EXISTS extension_failure_stage LowCardinality(String) DEFAULT '' AFTER timeout_stage;
