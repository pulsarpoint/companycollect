-- ── locations ─────────────────────────────────────────────────────────────────

-- name: UpsertCompanyLocation :one
INSERT INTO company_locations (
    company_id, location_type, label,
    address_line1, address_line2, city, region, postal_code,
    country, country_code, country_id, latitude, longitude,
    source, confidence, evidence
)
VALUES (
    $1, $2, $3,
    $4, $5, $6, $7, $8,
    $9, $10, (SELECT id FROM countries WHERE iso_alpha2 = $10), $11, $12,
    $13, $14, $15
)
ON CONFLICT (company_id, location_type, source)
    WHERE removed_at IS NULL AND location_type IN ('headquarters', 'registered_address')
DO UPDATE SET
    label         = EXCLUDED.label,
    address_line1 = EXCLUDED.address_line1,
    address_line2 = EXCLUDED.address_line2,
    city          = EXCLUDED.city,
    region        = EXCLUDED.region,
    postal_code   = EXCLUDED.postal_code,
    country       = EXCLUDED.country,
    country_code  = EXCLUDED.country_code,
    country_id    = EXCLUDED.country_id,
    latitude      = EXCLUDED.latitude,
    longitude     = EXCLUDED.longitude,
    confidence    = EXCLUDED.confidence,
    evidence      = EXCLUDED.evidence,
    removed_at    = NULL,
    updated_at    = now()
RETURNING *;

-- ── phones ────────────────────────────────────────────────────────────────────

-- name: UpsertCompanyPhone :one
INSERT INTO company_phones (company_id, phone, description, purpose, source, confidence, evidence)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (company_id, phone, purpose) WHERE removed_at IS NULL
DO UPDATE SET
    description = EXCLUDED.description,
    source      = EXCLUDED.source,
    confidence  = EXCLUDED.confidence,
    evidence    = EXCLUDED.evidence,
    removed_at  = NULL,
    updated_at  = now()
RETURNING *;

-- ── emails ────────────────────────────────────────────────────────────────────

-- name: UpsertCompanyEmail :one
INSERT INTO company_emails (company_id, email, description, purpose, name, source, confidence, evidence)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (company_id, lower(email), purpose) WHERE removed_at IS NULL
DO UPDATE SET
    description = EXCLUDED.description,
    name        = EXCLUDED.name,
    source      = EXCLUDED.source,
    confidence  = EXCLUDED.confidence,
    evidence    = EXCLUDED.evidence,
    removed_at  = NULL,
    updated_at  = now()
RETURNING *;

-- ── industries ────────────────────────────────────────────────────────────────

-- name: UpsertCompanyIndustry :one
INSERT INTO company_industries (company_id, industry, source, confidence, evidence)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT uq_company_industries
DO UPDATE SET
    source     = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    evidence   = EXCLUDED.evidence
RETURNING *;

-- ── markets ───────────────────────────────────────────────────────────────────

-- name: UpsertCompanyMarket :one
INSERT INTO company_markets (company_id, market, source, confidence, evidence)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT uq_company_markets
DO UPDATE SET
    source     = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    evidence   = EXCLUDED.evidence
RETURNING *;

-- ── services ──────────────────────────────────────────────────────────────────

-- name: UpsertCompanyService :one
INSERT INTO company_services (company_id, service, description, source, confidence, evidence)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT uq_company_services
DO UPDATE SET
    description = EXCLUDED.description,
    source      = EXCLUDED.source,
    confidence  = EXCLUDED.confidence,
    evidence    = EXCLUDED.evidence
RETURNING *;

-- name: UpdateCompanyEnrichment :one
-- ── enrichment update ─────────────────────────────────────────────────────────
UPDATE companies SET
    short_name        = COALESCE(sqlc.narg('short_name')::text,         short_name),
    short_description = COALESCE(sqlc.narg('short_description')::text,  short_description),
    description       = COALESCE(sqlc.narg('description')::text,        description),
    website           = COALESCE(sqlc.narg('website')::text,            website),
    founded_year      = COALESCE(sqlc.narg('founded_year')::int,        founded_year),
    employee_estimate = COALESCE(sqlc.narg('employee_estimate')::jsonb, employee_estimate),
    revenue_estimate  = COALESCE(sqlc.narg('revenue_estimate')::jsonb,  revenue_estimate),
    ownership         = COALESCE(sqlc.narg('ownership')::jsonb,         ownership),
    employee_count    = COALESCE(sqlc.narg('employee_count')::int,      employee_count),
    revenue_usd       = COALESCE(sqlc.narg('revenue_usd')::bigint,      revenue_usd),
    updated_at        = now()
WHERE id = sqlc.arg('id')::uuid
RETURNING *;

-- name: UpdateCompanyInfo :one
UPDATE companies SET
    name                = COALESCE(sqlc.narg('name')::text,              name),
    short_name          = COALESCE(sqlc.narg('short_name')::text,        short_name),
    short_description   = COALESCE(sqlc.narg('short_description')::text, short_description),
    description         = COALESCE(sqlc.narg('description')::text,       description),
    website             = COALESCE(sqlc.narg('website')::text,           website),
    founded_year        = COALESCE(sqlc.narg('founded_year')::int,       founded_year),
    updated_at          = now()
WHERE id = sqlc.arg('id')::uuid
RETURNING *;
