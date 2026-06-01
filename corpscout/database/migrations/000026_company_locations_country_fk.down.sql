DROP INDEX IF EXISTS idx_company_locations_country;
ALTER TABLE company_locations DROP COLUMN IF EXISTS country_id;
