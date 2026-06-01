-- Extend v_companies with all fields required by the company detail page.
-- Migration 017 recreated the view with a minimal column set after dropping
-- the old data_sources table; several columns the frontend expects were omitted.
-- Must DROP+CREATE because PostgreSQL disallows inserting/reordering columns
-- via CREATE OR REPLACE VIEW.
DROP VIEW IF EXISTS v_companies;
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
    c.description,
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
        SELECT NULLIF(TRIM(CONCAT_WS(', ', cl.city, cl.region, cl.country)), '')
        FROM company_locations cl
        WHERE cl.company_id = c.id
          AND cl.location_type = 'headquarters'
        LIMIT 1
    ) AS headquarters_location
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;
