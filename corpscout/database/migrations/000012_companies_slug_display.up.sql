-- database/migrations/000012_companies_slug_display.up.sql

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS canonical_slug     TEXT,
    ADD COLUMN IF NOT EXISTS display_name       TEXT,
    ADD COLUMN IF NOT EXISTS resolution_status  TEXT NOT NULL DEFAULT 'resolved',
    ADD COLUMN IF NOT EXISTS evidence           JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE companies
    ADD CONSTRAINT chk_companies_resolution_status
        CHECK (resolution_status IN ('resolved', 'draft', 'needs_review')),
    ADD CONSTRAINT chk_companies_evidence_object
        CHECK (jsonb_typeof(evidence) = 'object');

-- Step 1: backfill canonical_slug from display_name or name
-- Lowercase, replace & with 'and', strip non-alphanumeric, collapse runs to hyphens.
UPDATE companies
SET canonical_slug = trim(both '-' from
    regexp_replace(
        regexp_replace(
            lower(regexp_replace(COALESCE(display_name, name), '[&]', ' and ', 'g')),
            '[^a-z0-9]+', '-', 'g'
        ),
        '-{2,}', '-', 'g'
    )
)
WHERE canonical_slug IS NULL;

-- Fallback: any row with an empty slug after backfill gets a UUID-based slug
UPDATE companies
SET canonical_slug = 'company-' || left(replace(id::text, '-', ''), 8)
WHERE canonical_slug = '';

-- Step 2: resolve collisions by appending the first 12 chars of the company UUID (no dashes).
-- Any company that shares a slug with an earlier row (by created_at, then id) gets a suffix.
-- Using 12 chars (vs 8) makes UUID-prefix collisions essentially impossible.
WITH ranked AS (
    SELECT id,
           canonical_slug,
           ROW_NUMBER() OVER (PARTITION BY canonical_slug ORDER BY created_at, id) AS rn
    FROM companies
)
UPDATE companies c
SET canonical_slug = c.canonical_slug || '-' || left(replace(c.id::text, '-', ''), 12)
FROM ranked r
WHERE c.id = r.id
  AND r.rn > 1;

-- Step 3: enforce NOT NULL + unique index now that all rows have a slug
ALTER TABLE companies
    ALTER COLUMN canonical_slug SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_companies_canonical_slug
    ON companies(canonical_slug);

-- Refresh v_companies to expose new columns
DROP VIEW IF EXISTS v_companies;
CREATE VIEW v_companies AS
SELECT
    c.id,
    c.name,
    c.display_name,
    c.canonical_slug,
    c.resolution_status,
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
    c.evidence,
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
