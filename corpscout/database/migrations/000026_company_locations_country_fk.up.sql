-- Add country_id FK to countries, derived from country_code.
-- Nullable because some country codes in existing data are not yet in the
-- countries table (territories, dependencies, etc.).
ALTER TABLE company_locations
    ADD COLUMN country_id UUID REFERENCES countries(id);

UPDATE company_locations cl
SET country_id = c.id
FROM countries c
WHERE c.iso_alpha2 = cl.country_code;

CREATE INDEX idx_company_locations_country ON company_locations(country_id)
    WHERE country_id IS NOT NULL;
