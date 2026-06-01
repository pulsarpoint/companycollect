-- Restore v_company_sources as a compatibility view.
-- The old company_sources table (multi-source links) was dropped in migration 017.
-- The new schema stores a single primary_source_id on each company.
-- This view exposes that single link using the same column names as the original view
-- so the company detail page continues to work.
CREATE VIEW v_company_sources AS
SELECT
    c.id                AS company_id,
    NULL::text          AS external_id,
    NULL::timestamptz   AS fetched_at,
    ds.id               AS source_id,
    ds.name             AS source_name,
    ds.display_name     AS source_display_name,
    ds.source_group     AS source_type
FROM companies c
JOIN data_sources ds ON ds.id = c.primary_source_id
WHERE c.primary_source_id IS NOT NULL;

GRANT SELECT ON v_company_sources TO corpscout_anon;
