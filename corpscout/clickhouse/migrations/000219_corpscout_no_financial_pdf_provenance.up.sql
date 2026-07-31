CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.no_financial_reports
    ADD COLUMN IF NOT EXISTS source_file_name LowCardinality(String)
        DEFAULT concat(
            'aarsregnskap-',
            toString(source_filing_year),
            '_',
            org_number,
            '.pdf'
        )
        AFTER source_json_sha256;

ALTER TABLE corpscout.no_financial_facts
    ADD COLUMN IF NOT EXISTS source_file_name LowCardinality(String)
        DEFAULT concat(
            'aarsregnskap-',
            toString(source_filing_year),
            '_',
            org_number,
            '.pdf'
        )
        AFTER source_slug,
    ADD COLUMN IF NOT EXISTS source_url String
        DEFAULT concat(
            'https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/',
            org_number,
            '/',
            toString(source_filing_year)
        )
        AFTER source_file_name;

ALTER TABLE corpscout.no_financial_metrics
    ADD COLUMN IF NOT EXISTS source_file_name LowCardinality(String)
        DEFAULT concat(
            'aarsregnskap-',
            toString(source_filing_year),
            '_',
            org_number,
            '.pdf'
        )
        AFTER source_slug;

CREATE OR REPLACE VIEW corpscout.no_financial_facts_with_source AS
SELECT
    facts.*,
    reports.legal_name,
    reports.source_pdf_url,
    reports.source_pdf_sha256,
    reports.source_pdf_size_bytes,
    reports.source_json_object_key,
    reports.source_json_uri,
    reports.retrieved_at
FROM corpscout.no_financial_facts AS facts
INNER JOIN corpscout.no_financial_reports AS reports
    ON reports.document_id = facts.document_id;
