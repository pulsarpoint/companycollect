CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_bolagsverket_financial_observations
    ADD COLUMN IF NOT EXISTS source_reported_company_name Nullable(String)
        AFTER source_report_period_end,
    ADD COLUMN IF NOT EXISTS source_archive_key String
        AFTER source_reported_company_name,
    ADD COLUMN IF NOT EXISTS source_archive_name String
        AFTER source_archive_key,
    ADD COLUMN IF NOT EXISTS source_nested_zip_name String
        AFTER source_archive_name,
    ADD COLUMN IF NOT EXISTS source_xhtml_object_key String
        AFTER source_nested_zip_name,
    ADD COLUMN IF NOT EXISTS source_taxonomy_entrypoint Nullable(String)
        AFTER source_xhtml_object_key,
    ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64)
        AFTER source_taxonomy_entrypoint,
    ADD COLUMN IF NOT EXISTS source_fact_count UInt64
        AFTER source_payload_hash,
    ADD COLUMN IF NOT EXISTS source_unmapped_numeric_fact_count UInt64
        AFTER source_fact_count;
