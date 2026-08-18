ALTER TABLE corpscout.se_bolagsverket_financial_observations
    DROP COLUMN IF EXISTS source_unmapped_numeric_fact_count,
    DROP COLUMN IF EXISTS source_fact_count,
    DROP COLUMN IF EXISTS source_payload_hash,
    DROP COLUMN IF EXISTS source_taxonomy_entrypoint,
    DROP COLUMN IF EXISTS source_xhtml_object_key,
    DROP COLUMN IF EXISTS source_nested_zip_name,
    DROP COLUMN IF EXISTS source_archive_name,
    DROP COLUMN IF EXISTS source_archive_key,
    DROP COLUMN IF EXISTS source_reported_company_name;
