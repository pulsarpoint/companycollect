DROP VIEW IF EXISTS v_company_services;
DROP VIEW IF EXISTS v_company_markets;
DROP VIEW IF EXISTS v_company_industries;
DROP VIEW IF EXISTS v_company_emails;
DROP VIEW IF EXISTS v_company_phones;
DROP VIEW IF EXISTS v_company_locations;

-- Restore v_companies without enrichment columns
DROP VIEW IF EXISTS v_companies;
CREATE VIEW v_companies AS
SELECT
    c.id,
    c.name,
    c.registration_number,
    c.lei,
    c.status,
    c.created_at,
    c.updated_at,
    co.id           AS country_id,
    co.name         AS country_name,
    co.iso_alpha2   AS country_iso2,
    ds.name         AS primary_source,
    ds.display_name AS primary_source_display_name,
    (SELECT COUNT(*)::int FROM company_domains cd WHERE cd.company_id = c.id) AS domain_count
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;

DROP TABLE IF EXISTS company_services;
DROP TABLE IF EXISTS company_markets;
DROP TABLE IF EXISTS company_industries;
DROP TABLE IF EXISTS company_emails;
DROP TABLE IF EXISTS company_phones;
DROP TABLE IF EXISTS company_locations;

ALTER TABLE companies
    DROP CONSTRAINT IF EXISTS chk_companies_founded_year,
    DROP CONSTRAINT IF EXISTS chk_companies_employee_estimate_object,
    DROP CONSTRAINT IF EXISTS chk_companies_revenue_estimate_object,
    DROP CONSTRAINT IF EXISTS chk_companies_ownership_object,
    DROP COLUMN IF EXISTS short_name,
    DROP COLUMN IF EXISTS short_description,
    DROP COLUMN IF EXISTS description,
    DROP COLUMN IF EXISTS website,
    DROP COLUMN IF EXISTS founded_year,
    DROP COLUMN IF EXISTS employee_estimate,
    DROP COLUMN IF EXISTS revenue_estimate,
    DROP COLUMN IF EXISTS ownership;
