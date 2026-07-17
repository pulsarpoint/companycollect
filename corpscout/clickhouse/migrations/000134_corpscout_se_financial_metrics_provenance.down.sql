DROP VIEW IF EXISTS corpscout.se_financial_facts_with_source;

ALTER TABLE corpscout.se_financial_metrics
    DROP COLUMN IF EXISTS current_receivables_amount_usd,
    DROP COLUMN IF EXISTS current_receivables_amount_original,
    DROP COLUMN IF EXISTS taxonomy_entrypoint,
    DROP COLUMN IF EXISTS xhtml_source_uri,
    DROP COLUMN IF EXISTS xhtml_object_key,
    DROP COLUMN IF EXISTS nested_zip_name,
    DROP COLUMN IF EXISTS source_archive_name,
    DROP COLUMN IF EXISTS source_archive_key,
    DROP COLUMN IF EXISTS source_archive_url;
