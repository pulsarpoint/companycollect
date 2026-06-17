DROP TABLE IF EXISTS corpscout.fi_financial_metrics_long;
DROP TABLE IF EXISTS corpscout.fi_xbrl_taxonomy_codes;
DROP TABLE IF EXISTS corpscout.fi_xbrl_facts_raw;
DROP TABLE IF EXISTS corpscout.fi_xbrl_units;
DROP TABLE IF EXISTS corpscout.fi_xbrl_contexts;

ALTER TABLE corpscout.fi_financial_statements
    DROP COLUMN IF EXISTS root_name,
    DROP COLUMN IF EXISTS schema_refs,
    DROP COLUMN IF EXISTS taxonomy_entrypoint,
    DROP COLUMN IF EXISTS parsed_at;
