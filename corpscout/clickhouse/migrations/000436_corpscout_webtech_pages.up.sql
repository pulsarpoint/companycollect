-- Shadow schema only. Backfill, reconcile and explicitly cut over before changing canonical names.
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.webtech_domain_scan_results_v2
(
    crawl_id LowCardinality(String),
    root_domain String,
    website_origin String,
    page_url String,
    harmonic_rank UInt32,
    detector_version LowCardinality(String),
    partition_key LowCardinality(String),
    scan_id String,
    run_id String,
    outcome LowCardinality(String),
    timeout_stage LowCardinality(String),
    extension_failure_stage LowCardinality(String),
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
ORDER BY (root_domain, website_origin, page_url, crawl_id, detector_version, scan_id);

CREATE TABLE IF NOT EXISTS corpscout.webtech_domain_technologies_v2
(
    root_domain String,
    website_origin String,
    page_url String,
    crawl_id LowCardinality(String),
    detector_version LowCardinality(String),
    scan_id String,
    detected_name String,
    technology_id Nullable(UInt64),
    technology String,
    catalog_match LowCardinality(String),
    detected_slug String,
    version String,
    confidence UInt8,
    category_ids Array(UInt16),
    categories Array(String),
    category_slugs Array(String),
    requested_url String,
    final_url String,
    final_hostname String,
    outcome LowCardinality(String),
    analysis_status LowCardinality(String),
    analysis_complete UInt8,
    scanned_at DateTime64(3, 'UTC'),
    result_bucket LowCardinality(String),
    result_object_key String,
    report_sha256 FixedString(64),
    run_id String,
    recorded_at DateTime64(3, 'UTC'),
    CONSTRAINT confidence_range CHECK confidence <= 100,
    CONSTRAINT catalog_match_valid CHECK catalog_match IN ('exact', 'normalized', 'alias', 'unmapped', 'ambiguous')
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (root_domain, website_origin, page_url, crawl_id, detector_version, scan_id, detected_name);


CREATE VIEW IF NOT EXISTS corpscout.webtech_domain_technologies_current_v2 AS
SELECT d.root_domain,
    d.crawl_id,
    d.detector_version,
    d.scan_id,
    d.detected_name,
    d.technology_id,
    d.technology,
    d.catalog_match,
    d.detected_slug,
    d.version,
    d.confidence,
    d.category_ids,
    d.categories,
    d.category_slugs,
    d.requested_url,
    d.final_url,
    d.final_hostname,
    d.outcome,
    d.analysis_status,
    d.analysis_complete,
    d.scanned_at,
    d.result_bucket,
    d.result_object_key,
    d.report_sha256,
    d.run_id,
    d.recorded_at,
    d.website_origin,
    d.page_url
FROM corpscout.webtech_domain_technologies_v2 AS d FINAL
INNER JOIN (
    SELECT root_domain, website_origin, page_url, crawl_id, detector_version, scan_id, report_sha256
    FROM corpscout.webtech_domain_scan_results_v2 FINAL
    ORDER BY scanned_at DESC, crawl_id DESC, detector_version DESC, scan_id DESC, report_sha256 DESC
    LIMIT 1 BY root_domain, website_origin, page_url
) AS s
ON d.root_domain = s.root_domain AND d.website_origin = s.website_origin AND d.page_url = s.page_url
AND d.crawl_id = s.crawl_id AND d.detector_version = s.detector_version
AND d.scan_id = s.scan_id AND d.report_sha256 = s.report_sha256;
