-- database/migrations/000013_company_relationships.up.sql

CREATE TABLE company_relationships (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    related_company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    relationship_type   TEXT NOT NULL,
    ownership_percentage NUMERIC(5,2),
    valid_from          DATE,
    valid_to            DATE,
    source              TEXT NOT NULL DEFAULT 'manual',
    confidence          REAL,
    evidence            JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL DEFAULT 'active',
    removed_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_relationships_type CHECK (
        relationship_type IN (
            'direct_parent', 'ultimate_parent', 'subsidiary_of',
            'owned_by', 'acquired_by', 'merged_into', 'same_group', 'other'
        )
    ),
    CONSTRAINT chk_company_relationships_distinct CHECK (
        subject_company_id <> related_company_id
    ),
    CONSTRAINT chk_company_relationships_ownership_percentage CHECK (
        ownership_percentage IS NULL OR ownership_percentage BETWEEN 0 AND 100
    ),
    CONSTRAINT chk_company_relationships_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_relationships_status CHECK (
        status IN ('active', 'needs_review', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_relationships_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object'
    ),
    CONSTRAINT chk_company_relationships_valid_range CHECK (
        valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from
    )
);

CREATE UNIQUE INDEX uq_company_relationships_current
    ON company_relationships(subject_company_id, related_company_id, relationship_type)
    WHERE removed_at IS NULL
      AND status IN ('active', 'needs_review');

CREATE INDEX idx_company_relationships_subject
    ON company_relationships(subject_company_id, status, relationship_type)
    WHERE removed_at IS NULL;

CREATE INDEX idx_company_relationships_related
    ON company_relationships(related_company_id, status, relationship_type)
    WHERE removed_at IS NULL;

GRANT SELECT ON company_relationships TO corpscout_anon;
