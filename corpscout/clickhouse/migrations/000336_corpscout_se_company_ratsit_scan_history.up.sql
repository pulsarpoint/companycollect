CREATE DATABASE IF NOT EXISTS corpscout;

-- The previous direct-Dagster table retained only the newest successful report
-- per company. Promote it to immutable scan history while preserving those pilot
-- rows. Dagster's run UUID is the scan ID and is part of the replacement key.
DROP TABLE IF EXISTS corpscout.se_company_ratsit_crawl_results_next;
DROP TABLE IF EXISTS corpscout.se_company_ratsit_crawl_results_legacy;

CREATE TABLE corpscout.se_company_ratsit_crawl_results_next
(
    scan_id String CODEC(ZSTD(3)),
    company_id String CODEC(ZSTD(3)),
    outcome LowCardinality(String),
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
    CONSTRAINT se_company_ratsit_outcome CHECK outcome IN
        ('success', 'navigation', 'http', 'parse'),
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
        diagnostic_object_key = '' OR outcome = 'parse',
    CONSTRAINT se_company_ratsit_versions CHECK
        schema_version > 0 AND parser_version != '',
    CONSTRAINT se_company_ratsit_error CHECK
        (outcome = 'success' AND error_message = '')
        OR (outcome != 'success' AND error_message != ''),
    CONSTRAINT se_company_ratsit_timestamps CHECK fetched_at <= recorded_at
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (scan_id, company_id);

INSERT INTO corpscout.se_company_ratsit_crawl_results_next
(
    scan_id,
    company_id,
    outcome,
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
FROM corpscout.se_company_ratsit_crawl_results FINAL;

-- One row per Dagster run makes a scan discoverable without deriving it from
-- company rows. Failed scans may have zero company results but remain visible.
CREATE TABLE IF NOT EXISTS corpscout.se_company_ratsit_scans
(
    scan_id String CODEC(ZSTD(3)),
    status LowCardinality(String),
    selected_company_ids Array(String) CODEC(ZSTD(3)),
    selected_company_count UInt16,
    result_count UInt16,
    success_count UInt16,
    failure_count UInt16,
    reused_report_count UInt16,
    written_object_count UInt16,
    result_bucket LowCardinality(String),
    result_prefix String CODEC(ZSTD(3)),
    schema_version UInt16,
    parser_version LowCardinality(String),
    started_at DateTime64(6, 'UTC'),
    completed_at DateTime64(6, 'UTC'),
    error_type LowCardinality(String),
    error_message String CODEC(ZSTD(3)),
    recorded_at DateTime64(6, 'UTC'),
    CONSTRAINT se_company_ratsit_scan_identifier CHECK scan_id != '',
    CONSTRAINT se_company_ratsit_scan_status CHECK status IN
        ('success', 'completed_with_failures', 'failed'),
    CONSTRAINT se_company_ratsit_scan_selection CHECK
        selected_company_count = length(selected_company_ids)
        AND selected_company_count > 0
        AND selected_company_count <= 20
        AND length(arrayDistinct(selected_company_ids)) = selected_company_count,
    CONSTRAINT se_company_ratsit_scan_counts CHECK
        result_count = success_count + failure_count
        AND result_count <= selected_company_count
        AND reused_report_count <= success_count,
    CONSTRAINT se_company_ratsit_scan_completion CHECK
        (status = 'success'
            AND result_count = selected_company_count
            AND failure_count = 0
            AND error_type = ''
            AND error_message = '')
        OR (status = 'completed_with_failures'
            AND result_count = selected_company_count
            AND failure_count > 0
            AND error_type = ''
            AND error_message = '')
        OR (status = 'failed'
            AND error_type != ''
            AND error_message != ''),
    CONSTRAINT se_company_ratsit_scan_location CHECK
        result_bucket != '' AND result_prefix != '',
    CONSTRAINT se_company_ratsit_scan_versions CHECK
        schema_version > 0 AND parser_version != '',
    CONSTRAINT se_company_ratsit_scan_timestamps CHECK
        started_at <= completed_at AND completed_at <= recorded_at
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY scan_id;

INSERT INTO corpscout.se_company_ratsit_scans
(
    scan_id,
    status,
    selected_company_ids,
    selected_company_count,
    result_count,
    success_count,
    failure_count,
    reused_report_count,
    written_object_count,
    result_bucket,
    result_prefix,
    schema_version,
    parser_version,
    started_at,
    completed_at,
    error_type,
    error_message,
    recorded_at
)
SELECT
    dagster_run_id,
    'success',
    arraySort(groupArray(company_id)),
    toUInt16(count()),
    toUInt16(count()),
    toUInt16(count()),
    toUInt16(0),
    toUInt16(0),
    toUInt16(count()),
    any(report_bucket),
    'sweden_ratsit/pilot/',
    max(schema_version),
    any(parser_version),
    min(fetched_at),
    max(fetched_at),
    '',
    '',
    max(recorded_at)
FROM corpscout.se_company_ratsit_crawl_results FINAL
GROUP BY dagster_run_id;

DROP VIEW IF EXISTS corpscout.se_company_ratsit_current;
RENAME TABLE
    corpscout.se_company_ratsit_crawl_results
        TO corpscout.se_company_ratsit_crawl_results_legacy,
    corpscout.se_company_ratsit_crawl_results_next
        TO corpscout.se_company_ratsit_crawl_results;
DROP TABLE corpscout.se_company_ratsit_crawl_results_legacy;

-- Latest attempt and latest successful report are both exposed. A failed newest
-- attempt never hides the last usable report pointer.
CREATE VIEW corpscout.se_company_ratsit_current AS
SELECT
    company_id,
    latest.1 AS latest_scan_id,
    latest.2 AS latest_outcome,
    latest.3 AS latest_requested_url,
    latest.4 AS latest_source_url,
    latest.5 AS latest_http_status,
    latest.6 AS latest_result_bucket,
    latest.7 AS latest_result_object_key,
    latest.8 AS latest_result_sha256,
    latest.9 AS latest_result_size_bytes,
    latest.10 AS latest_report_reused,
    latest.11 AS latest_source_html_sha256,
    latest.12 AS latest_diagnostic_object_key,
    latest.13 AS schema_version,
    latest.14 AS parser_version,
    latest.15 AS latest_fetched_at,
    latest.16 AS latest_error_message,
    latest.17 AS latest_recorded_at,
    total_scan_count,
    successful_scan_count,
    if(successful_scan_count = 0, NULL, successful.1) AS report_scan_id,
    if(successful_scan_count = 0, NULL, successful.2) AS report_bucket,
    if(successful_scan_count = 0, NULL, successful.3) AS report_object_key,
    if(successful_scan_count = 0, NULL, successful.4) AS report_sha256,
    if(successful_scan_count = 0, NULL, successful.5) AS report_size_bytes,
    if(successful_scan_count = 0, NULL, successful.6) AS report_fetched_at
FROM
(
    SELECT
        company_id,
        argMax(
            tuple(
                scan_id,
                outcome,
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
            ),
            tuple(fetched_at, recorded_at, scan_id)
        ) AS latest,
        argMaxIf(
            tuple(
                scan_id,
                result_bucket,
                result_object_key,
                result_sha256,
                result_size_bytes,
                fetched_at
            ),
            tuple(fetched_at, recorded_at, scan_id),
            outcome = 'success'
        ) AS successful,
        count() AS total_scan_count,
        countIf(outcome = 'success') AS successful_scan_count
    FROM corpscout.se_company_ratsit_crawl_results FINAL
    GROUP BY company_id
);
