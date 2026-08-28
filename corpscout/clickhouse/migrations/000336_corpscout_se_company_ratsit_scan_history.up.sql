CREATE DATABASE IF NOT EXISTS corpscout;

-- The previous direct-Dagster table retained only the newest successful report
-- per company. Promote it to immutable scan history while preserving those pilot
-- rows. Dagster's run UUID is the scan ID and is part of the replacement key.
DROP TABLE IF EXISTS corpscout.se_company_ratsit_next;
DROP TABLE IF EXISTS corpscout.se_company_ratsit_legacy;

CREATE TABLE corpscout.se_company_ratsit_next
(
    scan_id String CODEC(ZSTD(3)),
    company_id String CODEC(ZSTD(3)),
    outcome LowCardinality(String),
    failure_type LowCardinality(String),
    requested_url String CODEC(ZSTD(3)),
    source_url String CODEC(ZSTD(3)),
    http_status Nullable(UInt16),
    result_bucket LowCardinality(String),
    result_object_key String CODEC(ZSTD(3)),
    result_sha256 FixedString(64),
    result_size_bytes UInt64,
    report_reused UInt8,
    source_html_sha256 Nullable(FixedString(64)),
    diagnostic_object_key String CODEC(ZSTD(3)),
    schema_version UInt16,
    parser_version LowCardinality(String),
    fetched_at DateTime64(6, 'UTC'),
    error_message String CODEC(ZSTD(3)),
    recorded_at DateTime64(6, 'UTC'),
    CONSTRAINT se_company_ratsit_scan_id CHECK scan_id != '',
    CONSTRAINT se_company_ratsit_company_id CHECK
        match(company_id, '^[0-9]{10}$'),
    CONSTRAINT se_company_ratsit_outcome CHECK outcome IN ('success', 'failure'),
    CONSTRAINT se_company_ratsit_failure_type CHECK
        (outcome = 'success' AND failure_type = '')
        OR (
            outcome = 'failure'
            AND failure_type IN ('navigation', 'http', 'parse')
        ),
    CONSTRAINT se_company_ratsit_requested_url CHECK
        requested_url = concat('https://www.ratsit.se/', right(company_id, 10)),
    CONSTRAINT se_company_ratsit_source_url CHECK
        startsWith(source_url, 'https://www.ratsit.se/'),
    CONSTRAINT se_company_ratsit_result_location CHECK
        result_bucket != ''
        AND startsWith(
            result_object_key,
            concat('sweden_ratsit/pilot/company_id=', company_id, '/')
        ),
    CONSTRAINT se_company_ratsit_result_hash CHECK
        match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_result_size CHECK result_size_bytes > 0,
    CONSTRAINT se_company_ratsit_report_reused CHECK
        report_reused IN (0, 1)
        AND (report_reused = 0 OR outcome = 'success'),
    CONSTRAINT se_company_ratsit_html_hash CHECK
        source_html_sha256 IS NULL
        OR match(toString(source_html_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_company_ratsit_diagnostic CHECK
        diagnostic_object_key = '' OR failure_type = 'parse',
    CONSTRAINT se_company_ratsit_versions CHECK
        schema_version > 0 AND parser_version != '',
    CONSTRAINT se_company_ratsit_error CHECK
        (outcome = 'success' AND error_message = '')
        OR (outcome != 'success' AND error_message != ''),
    CONSTRAINT se_company_ratsit_timestamps CHECK fetched_at <= recorded_at
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (scan_id, company_id);

INSERT INTO corpscout.se_company_ratsit_next
(
    scan_id,
    company_id,
    outcome,
    failure_type,
    requested_url,
    source_url,
    http_status,
    result_bucket,
    result_object_key,
    result_sha256,
    result_size_bytes,
    report_reused,
    source_html_sha256,
    diagnostic_object_key,
    schema_version,
    parser_version,
    fetched_at,
    error_message,
    recorded_at
)
SELECT
    dagster_run_id,
    company_id,
    'success',
    '',
    requested_url,
    source_url,
    toUInt16(200),
    report_bucket,
    report_object_key,
    report_sha256,
    report_size_bytes,
    toUInt8(0),
    source_html_sha256,
    '',
    schema_version,
    parser_version,
    fetched_at,
    '',
    recorded_at
FROM corpscout.se_company_ratsit FINAL;

RENAME TABLE
    corpscout.se_company_ratsit
        TO corpscout.se_company_ratsit_legacy,
    corpscout.se_company_ratsit_next
        TO corpscout.se_company_ratsit;
DROP TABLE corpscout.se_company_ratsit_legacy;
