ALTER TABLE corpscout.wikidata_companies
    DROP COLUMN IF EXISTS country_resolution_confidence,
    DROP COLUMN IF EXISTS country_resolution_method,
    DROP COLUMN IF EXISTS headquarters_country_iso2,
    DROP COLUMN IF EXISTS headquarters_country_label,
    DROP COLUMN IF EXISTS headquarters_country_wikidata_id,
    DROP COLUMN IF EXISTS headquarters_wikidata_id,
    DROP COLUMN IF EXISTS company_description;
