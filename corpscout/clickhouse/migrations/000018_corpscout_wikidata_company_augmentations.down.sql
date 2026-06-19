ALTER TABLE corpscout.wikidata_company_relationships
    DROP COLUMN IF EXISTS wikidata_property_id;

ALTER TABLE corpscout.wikidata_company_identifiers
    DROP COLUMN IF EXISTS wikidata_property_id;

ALTER TABLE corpscout.wikidata_companies
    DROP COLUMN IF EXISTS industry_wikidata_id,
    DROP COLUMN IF EXISTS logo_image_url,
    DROP COLUMN IF EXISTS logo_image,
    DROP COLUMN IF EXISTS employee_count_point_in_time,
    DROP COLUMN IF EXISTS employee_count,
    DROP COLUMN IF EXISTS legal_form_label,
    DROP COLUMN IF EXISTS legal_form_wikidata_id,
    DROP COLUMN IF EXISTS inception_date;
