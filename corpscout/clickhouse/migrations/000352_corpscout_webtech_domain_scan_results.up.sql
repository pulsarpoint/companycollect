CREATE DATABASE IF NOT EXISTS corpscout;

-- Resumability and serving index for the CloakBrowser/Wappalyzer scan. The full
-- schema-v2 report remains in RustFS. This table keeps only fields needed to
-- select unfinished domains and inspect pilot outcomes. Replacing the same
-- (crawl, domain, detector) identity makes a forced rescan deterministic.
CREATE TABLE IF NOT EXISTS corpscout.webtech_domain_scan_results
(
    crawl_id LowCardinality(String),
    root_domain String,
    harmonic_rank UInt32,
    detector_version LowCardinality(String),
    partition_key LowCardinality(String),
    run_id String,
    outcome LowCardinality(String),
    requested_url String,
    final_url String,
    final_hostname String,
    http_fallback_used UInt8,
    technology_count UInt16,
    result_bucket LowCardinality(String),
    result_object_key String,
    report_sha256 FixedString(64),
    report_size_bytes UInt32,
    scanned_at DateTime64(3, 'UTC'),
    duration_ms UInt32,
    error_message String,
    recorded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (crawl_id, root_domain, detector_version);
