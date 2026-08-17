CREATE DATABASE IF NOT EXISTS corpscout;

-- Explicit observations only. `unknown` is deliberately absent: it is the
-- result of having neither structured financial data nor official filing
-- evidence, not a fact supplied by a source.
CREATE TABLE IF NOT EXISTS corpscout.se_annual_report_filing_observations
(
    company_id String,
    report_period_end Nullable(Date32),
    filing_status LowCardinality(String),
    filing_registered_on Nullable(Date32),
    source_file_format LowCardinality(Nullable(String)),
    bolagsverket_document_id Nullable(String),
    source_slug LowCardinality(String),
    source_record_id String,
    source_url String,
    source_object_key String,
    source_payload_sha256 String,
    source_run_id String,
    observed_at DateTime64(3, 'UTC'),
    CONSTRAINT filing_status_is_supported CHECK filing_status IN (
        'filed_unstructured',
        'not_submitted'
    ),
    CONSTRAINT not_submitted_has_report_period CHECK
        filing_status != 'not_submitted' OR report_period_end IS NOT NULL,
    CONSTRAINT filing_observation_has_source_identity CHECK
        source_slug != '' AND source_record_id != ''
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (
    company_id,
    ifNull(report_period_end, toDate32('1970-01-01')),
    source_slug,
    source_record_id
);

-- One current, evidence-backed state per company. Structured financial data
-- is a candidate alongside explicit observations, so a newer report period
-- can replace an older format. For the same period, data availability wins
-- over stale missing/unstructured observations regardless of load order.
CREATE OR REPLACE VIEW corpscout.se_annual_report_filing_status_current AS
SELECT
    company_id,
    latest.1 AS filing_status,
    latest.2 AS report_period_end,
    latest.3 AS filing_registered_on,
    latest.4 AS source_file_format,
    latest.5 AS bolagsverket_document_id,
    latest.6 AS source_slug,
    latest.7 AS source_record_id,
    latest.8 AS source_url,
    latest.9 AS source_object_key,
    latest.10 AS source_payload_sha256,
    latest.11 AS source_run_id,
    latest.12 AS observed_at
FROM
(
    SELECT
        company_id,
        argMax(
            tuple(
                filing_status,
                report_period_end,
                filing_registered_on,
                source_file_format,
                bolagsverket_document_id,
                source_slug,
                source_record_id,
                source_url,
                source_object_key,
                source_payload_sha256,
                source_run_id,
                observed_at
            ),
            tuple(
                ifNull(report_period_end, toDate32('1970-01-01')),
                toUInt8(filing_status = 'data_available'),
                observed_at,
                source_slug,
                source_record_id
            )
        ) AS latest
    FROM
    (
        SELECT
            company_id,
            'data_available' AS filing_status,
            toDate32(period_end_date) AS report_period_end,
            CAST(NULL, 'Nullable(Date32)') AS filing_registered_on,
            CAST('application/xhtml+xml' AS Nullable(String)) AS source_file_format,
            CAST(NULL, 'Nullable(String)') AS bolagsverket_document_id,
            'sweden_financial' AS source_slug,
            concat('financials-latest:', company_id) AS source_record_id,
            'https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler' AS source_url,
            '' AS source_object_key,
            '' AS source_payload_sha256,
            '' AS source_run_id,
            resolved_at AS observed_at
        FROM corpscout.se_company_financials_latest

        UNION ALL

        SELECT
            company_id,
            filing_status,
            report_period_end,
            filing_registered_on,
            source_file_format,
            bolagsverket_document_id,
            source_slug,
            source_record_id,
            source_url,
            source_object_key,
            source_payload_sha256,
            source_run_id,
            observed_at
        FROM corpscout.se_annual_report_filing_observations FINAL
    )
    GROUP BY company_id
);
