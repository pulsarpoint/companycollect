-- name: UpsertCompanyRelationship :one
INSERT INTO company_relationships (
    subject_company_id, related_company_id, relationship_type,
    source, confidence, evidence, ownership_percentage, valid_from, valid_to
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT ON CONSTRAINT uq_company_relationships_current
DO UPDATE SET
    source               = EXCLUDED.source,
    confidence           = EXCLUDED.confidence,
    evidence             = EXCLUDED.evidence,
    ownership_percentage = EXCLUDED.ownership_percentage,
    valid_from           = EXCLUDED.valid_from,
    valid_to             = EXCLUDED.valid_to,
    updated_at           = now()
RETURNING *;
