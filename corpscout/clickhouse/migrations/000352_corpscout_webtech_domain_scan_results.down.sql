CREATE DATABASE IF NOT EXISTS corpscout;

-- The complete reports remain recoverable from RustFS if this index is retired.
DROP TABLE IF EXISTS corpscout.webtech_domain_scan_results;
