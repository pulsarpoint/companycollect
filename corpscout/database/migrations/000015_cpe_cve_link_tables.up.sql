-- database/migrations/000015_cpe_cve_link_tables.up.sql

-- ── cpe_entity_link_suggestions ───────────────────────────────────────────────
CREATE TABLE cpe_entity_link_suggestions (
    id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cpe_vendor_token              TEXT NOT NULL,
    target_entity_type            TEXT NOT NULL,
    target_company_id             UUID REFERENCES companies(id) ON DELETE SET NULL,
    target_organization_id        UUID REFERENCES organizations(id) ON DELETE SET NULL,
    target_open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE SET NULL,
    proposed_entity_payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    suggested_by                  TEXT NOT NULL,
    confidence                    REAL,
    evidence                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                        TEXT NOT NULL DEFAULT 'pending',
    reviewed_by                   TEXT,
    reviewed_at                   TIMESTAMPTZ,
    review_note                   TEXT,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_cpe_entity_link_suggestions_token CHECK (btrim(cpe_vendor_token) <> ''),
    CONSTRAINT chk_cpe_entity_link_suggestions_target_type CHECK (
        target_entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_target_matches_type CHECK (
        (target_entity_type = 'company'
            AND target_organization_id IS NULL
            AND target_open_source_project_id IS NULL)
        OR (target_entity_type = 'organization'
            AND target_company_id IS NULL
            AND target_open_source_project_id IS NULL)
        OR (target_entity_type = 'open_source_project'
            AND target_company_id IS NULL
            AND target_organization_id IS NULL)
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_target_or_payload CHECK (
        (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 1
            AND proposed_entity_payload = '{}'::jsonb
        )
        OR (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 0
            AND proposed_entity_payload <> '{}'::jsonb
        )
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_cpe_entity_link_suggestions_payload_object  CHECK (jsonb_typeof(proposed_entity_payload) = 'object')
);

CREATE INDEX idx_cpe_entity_link_suggestions_review
    ON cpe_entity_link_suggestions(status, cpe_vendor_token, target_entity_type);

-- ── cpe_entity_links ──────────────────────────────────────────────────────────
CREATE TABLE cpe_entity_links (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cpe_vendor_token      TEXT NOT NULL,
    entity_type           TEXT NOT NULL,
    company_id            UUID REFERENCES companies(id) ON DELETE CASCADE,
    organization_id       UUID REFERENCES organizations(id) ON DELETE CASCADE,
    open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE CASCADE,
    approved_suggestion_id UUID NOT NULL REFERENCES cpe_entity_link_suggestions(id),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    removed_at            TIMESTAMPTZ,
    CONSTRAINT chk_cpe_entity_links_token CHECK (btrim(cpe_vendor_token) <> ''),
    CONSTRAINT chk_cpe_entity_links_entity_type CHECK (
        entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cpe_entity_links_one_target CHECK (
        num_nonnulls(company_id, organization_id, open_source_project_id) = 1
    ),
    CONSTRAINT chk_cpe_entity_links_type_matches_target CHECK (
        (entity_type = 'company' AND company_id IS NOT NULL)
        OR (entity_type = 'organization' AND organization_id IS NOT NULL)
        OR (entity_type = 'open_source_project' AND open_source_project_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_cpe_entity_links_active_token
    ON cpe_entity_links(cpe_vendor_token)
    WHERE removed_at IS NULL;

CREATE UNIQUE INDEX uq_cpe_entity_links_active_suggestion
    ON cpe_entity_links(approved_suggestion_id)
    WHERE removed_at IS NULL;

-- ── cve_entity_link_suggestions ───────────────────────────────────────────────
CREATE TABLE cve_entity_link_suggestions (
    id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cve_id                        TEXT NOT NULL,
    target_entity_type            TEXT NOT NULL,
    target_company_id             UUID REFERENCES companies(id) ON DELETE SET NULL,
    target_organization_id        UUID REFERENCES organizations(id) ON DELETE SET NULL,
    target_open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE SET NULL,
    proposed_entity_payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    suggested_by                  TEXT NOT NULL,
    confidence                    REAL,
    evidence                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                        TEXT NOT NULL DEFAULT 'pending',
    reviewed_by                   TEXT,
    reviewed_at                   TIMESTAMPTZ,
    review_note                   TEXT,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_cve_entity_link_suggestions_cve_id CHECK (
        cve_id ~ '^CVE-[0-9]{4}-[0-9]{4,}$'
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_target_type CHECK (
        target_entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_target_matches_type CHECK (
        (target_entity_type = 'company'
            AND target_organization_id IS NULL
            AND target_open_source_project_id IS NULL)
        OR (target_entity_type = 'organization'
            AND target_company_id IS NULL
            AND target_open_source_project_id IS NULL)
        OR (target_entity_type = 'open_source_project'
            AND target_company_id IS NULL
            AND target_organization_id IS NULL)
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_target_or_payload CHECK (
        (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 1
            AND proposed_entity_payload = '{}'::jsonb
        )
        OR (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 0
            AND proposed_entity_payload <> '{}'::jsonb
        )
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_cve_entity_link_suggestions_payload_object  CHECK (jsonb_typeof(proposed_entity_payload) = 'object')
);

CREATE INDEX idx_cve_entity_link_suggestions_review
    ON cve_entity_link_suggestions(status, cve_id, target_entity_type);

-- ── cve_entity_links ──────────────────────────────────────────────────────────
CREATE TABLE cve_entity_links (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cve_id                TEXT NOT NULL,
    entity_type           TEXT NOT NULL,
    company_id            UUID REFERENCES companies(id) ON DELETE CASCADE,
    organization_id       UUID REFERENCES organizations(id) ON DELETE CASCADE,
    open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE CASCADE,
    approved_suggestion_id UUID NOT NULL REFERENCES cve_entity_link_suggestions(id),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    removed_at            TIMESTAMPTZ,
    CONSTRAINT chk_cve_entity_links_cve_id CHECK (
        cve_id ~ '^CVE-[0-9]{4}-[0-9]{4,}$'
    ),
    CONSTRAINT chk_cve_entity_links_entity_type CHECK (
        entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cve_entity_links_one_target CHECK (
        num_nonnulls(company_id, organization_id, open_source_project_id) = 1
    ),
    CONSTRAINT chk_cve_entity_links_type_matches_target CHECK (
        (entity_type = 'company' AND company_id IS NOT NULL)
        OR (entity_type = 'organization' AND organization_id IS NOT NULL)
        OR (entity_type = 'open_source_project' AND open_source_project_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_cve_entity_links_active_company
    ON cve_entity_links(cve_id, company_id)
    WHERE removed_at IS NULL AND company_id IS NOT NULL;

CREATE UNIQUE INDEX uq_cve_entity_links_active_organization
    ON cve_entity_links(cve_id, organization_id)
    WHERE removed_at IS NULL AND organization_id IS NOT NULL;

CREATE UNIQUE INDEX uq_cve_entity_links_active_open_source_project
    ON cve_entity_links(cve_id, open_source_project_id)
    WHERE removed_at IS NULL AND open_source_project_id IS NOT NULL;

CREATE UNIQUE INDEX uq_cve_entity_links_active_suggestion
    ON cve_entity_links(approved_suggestion_id)
    WHERE removed_at IS NULL;

GRANT SELECT ON cpe_entity_link_suggestions TO corpscout_anon;
GRANT SELECT ON cpe_entity_links            TO corpscout_anon;
GRANT SELECT ON cve_entity_link_suggestions TO corpscout_anon;
GRANT SELECT ON cve_entity_links            TO corpscout_anon;
