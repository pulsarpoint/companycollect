-- name: GetCompany :one
SELECT * FROM companies WHERE id = $1;

-- name: GetCompanyBySlug :one
SELECT * FROM companies WHERE canonical_slug = $1;

-- name: InsertCompany :one
INSERT INTO companies (canonical_slug, name, country_id, status)
VALUES ($1, $2, $3, coalesce($4, 'active'))
RETURNING *;

-- name: GetCompanyByExactName :one
SELECT * FROM companies WHERE lower(name) = lower($1) LIMIT 1;

-- name: InsertCompanyFromRawInput :one
INSERT INTO companies (
    canonical_slug, name, country_id, registration_number, lei,
    status, website, primary_source_id, parent_lei, ultimate_parent_lei, evidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING *;

-- name: UpdateCompanyRegistryProfile :one
UPDATE companies SET
    registration_number = COALESCE(sqlc.narg('registration_number')::text, registration_number),
    lei                 = COALESCE(sqlc.narg('lei')::text,                 lei),
    status              = COALESCE(sqlc.narg('status')::text,              status),
    parent_lei          = COALESCE(sqlc.narg('parent_lei')::text,          parent_lei),
    ultimate_parent_lei = COALESCE(sqlc.narg('ultimate_parent_lei')::text, ultimate_parent_lei),
    updated_at          = now()
WHERE id = sqlc.arg('id')::uuid
RETURNING *;
