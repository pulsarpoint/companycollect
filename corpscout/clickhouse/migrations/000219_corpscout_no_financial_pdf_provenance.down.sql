CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.no_financial_facts_with_source;

ALTER TABLE corpscout.no_financial_metrics
    DROP COLUMN IF EXISTS source_file_name;

ALTER TABLE corpscout.no_financial_facts
    DROP COLUMN IF EXISTS source_url,
    DROP COLUMN IF EXISTS source_file_name;

ALTER TABLE corpscout.no_financial_reports
    DROP COLUMN IF EXISTS source_file_name;

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
