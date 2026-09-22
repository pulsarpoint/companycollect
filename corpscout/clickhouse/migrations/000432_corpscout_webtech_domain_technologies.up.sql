-- Catalog names are the existing canonical identity. Derive the same ID on every
-- stage/exchange refresh; slugs cannot serve as IDs because they are not unique.
ALTER TABLE corpscout.technology_catalog
    ADD COLUMN IF NOT EXISTS technology_id UInt64 MATERIALIZED cityHash64(technology);

-- Immutable scan observations. Re-indexing a report replaces the same keys;
-- a later scan keeps its own history, including technologies it stopped detecting.
CREATE TABLE IF NOT EXISTS corpscout.webtech_domain_technologies
(
    root_domain String,
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
ORDER BY (root_domain, crawl_id, detector_version, scan_id, detected_name);

-- The result index is published AFTER all detections. Join against its current
-- scan so partial writes and technologies absent from a rescan are not served.
-- A zero-detection scan therefore correctly produces zero current rows.
CREATE VIEW IF NOT EXISTS corpscout.webtech_domain_technologies_current AS
SELECT d.*
FROM corpscout.webtech_domain_technologies AS d FINAL
INNER JOIN corpscout.webtech_domain_scan_results AS s FINAL
    ON d.root_domain = s.root_domain
    AND d.crawl_id = s.crawl_id
    AND d.detector_version = s.detector_version
    AND d.scan_id = s.scan_id
    AND d.report_sha256 = s.report_sha256;
