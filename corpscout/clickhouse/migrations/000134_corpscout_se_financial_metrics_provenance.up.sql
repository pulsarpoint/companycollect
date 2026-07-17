CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_financial_metrics
    ADD COLUMN IF NOT EXISTS source_archive_url String AFTER reported_company_name,
    ADD COLUMN IF NOT EXISTS source_archive_key String AFTER source_archive_url,
    ADD COLUMN IF NOT EXISTS source_archive_name String AFTER source_archive_key,
    ADD COLUMN IF NOT EXISTS nested_zip_name String AFTER source_archive_name,
    ADD COLUMN IF NOT EXISTS xhtml_object_key String AFTER nested_zip_name,
    ADD COLUMN IF NOT EXISTS xhtml_source_uri String AFTER xhtml_object_key,
    ADD COLUMN IF NOT EXISTS taxonomy_entrypoint Nullable(String) AFTER xhtml_source_uri,
    ADD COLUMN IF NOT EXISTS current_receivables_amount_original Nullable(Decimal(38, 6))
        AFTER current_assets_amount_usd,
    ADD COLUMN IF NOT EXISTS current_receivables_amount_usd Nullable(Decimal(38, 6))
        AFTER current_receivables_amount_original;

CREATE OR REPLACE VIEW corpscout.se_financial_facts_with_source AS
SELECT
    facts.*,
    reports.report_period_start,
    reports.fiscal_year,
    reports.reported_company_name,
    reports.report_language,
    concat(
        'https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/',
        'arsredovisningar/',
        extract(reports.source_archive_key, 'year=([^/]+)'),
        '/',
        reports.source_archive_name
    ) AS source_archive_url,
    reports.source_archive_key,
    reports.source_archive_name,
    reports.nested_zip_name,
    reports.xhtml_object_key,
    concat(
        's3://source-sweden-financial/',
        reports.xhtml_object_key
    ) AS xhtml_source_uri,
    reports.xhtml_sha256,
    reports.xhtml_size_bytes,
    reports.taxonomy_entrypoint,
    reports.schema_refs
FROM corpscout.se_financial_facts AS facts
LEFT ANY JOIN corpscout.se_financial_reports AS reports
    ON reports.statement_key = facts.statement_key;
