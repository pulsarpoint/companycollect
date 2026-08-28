CREATE DATABASE IF NOT EXISTS corpscout;

-- Roll back to the success-only report catalog introduced by migration 000334.
-- Failure history has no representation in that schema and is intentionally
-- omitted from the restored table.
DROP TABLE IF EXISTS corpscout.se_company_ratsit_crawl_results_next;
DROP TABLE IF EXISTS corpscout.se_company_ratsit_crawl_results_legacy;

CREATE TABLE corpscout.se_company_ratsit_crawl_results_next
(
    company_id String CODEC(ZSTD(3)),
    requested_url String CODEC(ZSTD(3)),
    source_url String CODEC(ZSTD(3)),
    report_bucket LowCardinality(String),
    report_object_key String CODEC(ZSTD(3)),
    report_sha256 FixedString(64),
    report_size_bytes UInt64,
    report_payload_sha256 FixedString(64),
    source_html_sha256 FixedString(64),
    schema_version UInt16,
    parser_version LowCardinality(String),
    dagster_run_id String CODEC(ZSTD(3)),
    fetched_at DateTime64(6, 'UTC'),
    recorded_at DateTime64(6, 'UTC'),
    CONSTRAINT se_company_ratsit_company_id CHECK
        match(company_id, '^[0-9]{10,12}$'),
    CONSTRAINT se_company_ratsit_requested_url CHECK
        requested_url = concat('https://www.ratsit.se/', right(company_id, 10)),
    CONSTRAINT se_company_ratsit_source_url CHECK
        startsWith(source_url, 'https://www.ratsit.se/'),
    CONSTRAINT se_company_ratsit_report_location CHECK
        report_bucket != ''
        AND startsWith(
            report_object_key,
            concat('sweden_ratsit/pilot/company_id=', company_id, '/')
        ),
    CONSTRAINT se_company_ratsit_report_sha256 CHECK
        match(toString(report_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_report_size CHECK report_size_bytes > 0,
    CONSTRAINT se_company_ratsit_payload_sha256 CHECK
        match(toString(report_payload_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_html_sha256 CHECK
        match(toString(source_html_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_schema_version CHECK schema_version > 0,
    CONSTRAINT se_company_ratsit_parser_version CHECK parser_version != '',
    CONSTRAINT se_company_ratsit_dagster_run CHECK dagster_run_id != '',
    CONSTRAINT se_company_ratsit_timestamps CHECK fetched_at <= recorded_at
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY company_id;

INSERT INTO corpscout.se_company_ratsit_crawl_results_next
(
    company_id,
    requested_url,
    source_url,
    report_bucket,
    report_object_key,
    report_sha256,
    report_size_bytes,
    report_payload_sha256,
    source_html_sha256,
    schema_version,
    parser_version,
    dagster_run_id,
    fetched_at,
    recorded_at
)
SELECT
    company_id,
    requested_url,
    source_url,
    result_bucket,
    result_object_key,
    result_sha256,
    result_size_bytes,
    result_sha256,
    ifNull(source_html_sha256, repeat('0', 64)),
    schema_version,
    parser_version,
    scan_id,
    fetched_at,
    recorded_at
FROM corpscout.se_company_ratsit_crawl_results FINAL
WHERE outcome = 'success';

DROP VIEW IF EXISTS corpscout.se_company_ratsit_current;
RENAME TABLE
    corpscout.se_company_ratsit_crawl_results
        TO corpscout.se_company_ratsit_crawl_results_legacy,
    corpscout.se_company_ratsit_crawl_results_next
        TO corpscout.se_company_ratsit_crawl_results;
DROP TABLE corpscout.se_company_ratsit_crawl_results_legacy;
DROP TABLE IF EXISTS corpscout.se_company_ratsit_scans;

CREATE VIEW corpscout.se_company_ratsit_current AS
SELECT
    company_id,
    requested_url,
    source_url,
    report_bucket,
    report_object_key,
    report_sha256,
    report_size_bytes,
    report_payload_sha256,
    source_html_sha256,
    schema_version,
    parser_version,
    dagster_run_id,
    fetched_at,
    recorded_at
FROM corpscout.se_company_ratsit_crawl_results FINAL;
