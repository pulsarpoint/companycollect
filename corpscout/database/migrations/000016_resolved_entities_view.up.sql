-- database/migrations/000016_resolved_entities_view.up.sql

CREATE VIEW v_resolved_entities AS
SELECT
    'company'::text AS entity_type,
    id AS entity_id,
    COALESCE(display_name, name) AS display_name,
    canonical_slug,
    website,
    status,
    updated_at
FROM companies
UNION ALL
SELECT
    'organization',
    id,
    display_name,
    canonical_slug,
    website,
    status,
    updated_at
FROM organizations
UNION ALL
SELECT
    'open_source_project',
    id,
    display_name,
    canonical_slug,
    website,
    status,
    updated_at
FROM open_source_projects;

GRANT SELECT ON v_resolved_entities TO corpscout_anon;
