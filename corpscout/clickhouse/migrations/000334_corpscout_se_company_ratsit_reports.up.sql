CREATE DATABASE IF NOT EXISTS corpscout;

-- The previous table contained Temporal crawl attempts and raw HTML envelope
-- pointers. Ratsit collection now runs directly in Dagster and publishes one
-- normalized report.json per company at a deterministic S3 key. The old rows
-- and schema are deliberately discarded rather than mixing two contracts.
DROP VIEW IF EXISTS corpscout.se_company_ratsit_current;
DROP TABLE IF EXISTS corpscout.se_company_ratsit_crawl_results;

-- Success-only discovery catalog for normalized Ratsit reports in S3. A newer
-- successful fetch replaces the current row for the company. Failed fetches do
-- not write here, so the last successful report remains discoverable.
CREATE TABLE IF NOT EXISTS corpscout.se_company_ratsit_crawl_results
(
    company_id String,
    requested_url String,
    source_url String,
    report_bucket LowCardinality(String),
    report_object_key String,
    report_sha256 FixedString(64),
    report_size_bytes UInt64,
    report_payload_sha256 FixedString(64),
    source_html_sha256 FixedString(64),
    schema_version UInt16,
    parser_version LowCardinality(String),
    dagster_run_id String,
    fetched_at DateTime64(6, 'UTC'),
    recorded_at DateTime64(6, 'UTC'),

    CONSTRAINT se_company_ratsit_company_id CHECK
        match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_company_ratsit_requested_url CHECK
        requested_url = concat(
            'https://www.ratsit.se/',
            right(company_id, 10)
        ),
    CONSTRAINT se_company_ratsit_source_url CHECK
        startsWith(source_url, requested_url),
    CONSTRAINT se_company_ratsit_report_location CHECK
        report_bucket != ''
        AND endsWith(
            report_object_key,
            concat('/company_id=', company_id, '/report.json')
        ),
    CONSTRAINT se_company_ratsit_report_sha256 CHECK
        match(report_sha256, '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_report_size CHECK
        report_size_bytes > 0,
    CONSTRAINT se_company_ratsit_payload_sha256 CHECK
        match(report_payload_sha256, '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_html_sha256 CHECK
        match(source_html_sha256, '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_schema_version CHECK
        schema_version > 0,
    CONSTRAINT se_company_ratsit_parser_version CHECK
        parser_version != '',
    CONSTRAINT se_company_ratsit_dagster_run CHECK
        dagster_run_id != '',
    CONSTRAINT se_company_ratsit_timestamps CHECK
        fetched_at <= recorded_at
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY company_id;

-- Consumers use this view for one current report pointer per company and can
-- fetch the exact bucket and key without listing the S3 prefix.
CREATE VIEW IF NOT EXISTS corpscout.se_company_ratsit_current AS
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
