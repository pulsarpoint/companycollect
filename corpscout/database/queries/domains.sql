-- name: UpsertDomain :one
INSERT INTO domains (domain)
VALUES ($1)
ON CONFLICT (domain) DO UPDATE SET last_verified_at = now()
RETURNING *;

-- name: UpsertCompanyDomain :one
INSERT INTO company_domains (company_id, domain_id, relationship_type, status, signal, confidence, evidence)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (company_id, domain_id, signal) DO UPDATE SET
    confidence   = EXCLUDED.confidence,
    evidence     = EXCLUDED.evidence,
    last_seen_at = now()
RETURNING *;

-- name: ListDomains :many
SELECT d.domain, c.name AS company_name, cd.*
FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'))
  AND (sqlc.narg('q')::text IS NULL
       OR c.name ILIKE '%' || sqlc.narg('q') || '%'
       OR d.domain ILIKE '%' || sqlc.narg('q') || '%')
ORDER BY cd.confidence DESC, d.domain
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountDomains :one
SELECT COUNT(*) FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'))
  AND (sqlc.narg('q')::text IS NULL
       OR c.name ILIKE '%' || sqlc.narg('q') || '%'
       OR d.domain ILIKE '%' || sqlc.narg('q') || '%');

-- name: ListReviewCandidateIDs :many
SELECT cd.id FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'))
  AND (sqlc.narg('q')::text IS NULL
       OR c.name ILIKE '%' || sqlc.narg('q') || '%'
       OR d.domain ILIKE '%' || sqlc.narg('q') || '%')
ORDER BY cd.confidence DESC, d.domain;

-- name: ReviewCompanyDomain :exec
UPDATE company_domains SET status = $2 WHERE id = $1;

-- name: GetDomainByID :one
SELECT id, domain, first_seen_at, last_verified_at
FROM domains
WHERE id = $1;

-- name: UpsertDomainWithSource :one
INSERT INTO domains (domain, import_source)
VALUES ($1, $2)
ON CONFLICT (domain) DO UPDATE SET last_verified_at = now()
RETURNING *;
