DROP VIEW IF EXISTS corpscout.fi_financial_facts_with_source;
DROP TABLE IF EXISTS corpscout.fi_xbrl_taxonomy_codes;
DROP TABLE IF EXISTS corpscout.fi_xbrl_facts_raw;
DROP TABLE IF EXISTS corpscout.fi_xbrl_units;
DROP TABLE IF EXISTS corpscout.fi_xbrl_contexts;

ALTER TABLE corpscout.fi_financial_metrics
    DROP COLUMN IF EXISTS fx_source,
    DROP COLUMN IF EXISTS taxonomy_entrypoint,
    DROP COLUMN IF EXISTS xml_source_uri,
    DROP COLUMN IF EXISTS fiscal_year;
