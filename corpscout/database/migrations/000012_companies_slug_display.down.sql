-- database/migrations/000012_companies_slug_display.down.sql

DROP VIEW IF EXISTS v_companies;

-- Restore v_companies without new columns (matches state after 000009/000011)
CREATE VIEW v_companies AS
SELECT
    c.id,
    c.name,
    c.short_name,
    c.registration_number,
    c.lei,
    c.status,
    c.website,
    c.short_description,
    c.founded_year,
    c.employee_estimate,
    c.revenue_estimate,
    c.ownership,
    c.created_at,
    c.updated_at,
    co.id           AS country_id,
    co.name         AS country_name,
    co.iso_alpha2   AS country_iso2,
    ds.name         AS primary_source,
    ds.display_name AS primary_source_display_name,
    (SELECT COUNT(*)::int FROM company_domains cd WHERE cd.company_id = c.id) AS domain_count,
    (
        SELECT cl.city || COALESCE(', ' || cl.country_code, '')
        FROM company_locations cl
        WHERE cl.company_id = c.id AND cl.removed_at IS NULL AND cl.location_type = 'headquarters'
        LIMIT 1
    ) AS headquarters_location
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;

DROP INDEX IF EXISTS uq_companies_canonical_slug;

ALTER TABLE companies
    DROP CONSTRAINT IF EXISTS chk_companies_resolution_status,
    DROP CONSTRAINT IF EXISTS chk_companies_evidence_object,
    DROP COLUMN IF EXISTS canonical_slug,
    DROP COLUMN IF EXISTS display_name,
    DROP COLUMN IF EXISTS resolution_status,
    DROP COLUMN IF EXISTS evidence;
