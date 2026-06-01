-- database/migrations/000014_organizations_open_source_projects.up.sql

CREATE TABLE organizations (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_slug    TEXT NOT NULL UNIQUE,
    display_name      TEXT NOT NULL,
    organization_type TEXT NOT NULL,
    website           TEXT,
    short_description TEXT,
    description       TEXT,
    country_code      TEXT,
    governance        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence          JSONB NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT NOT NULL DEFAULT 'active',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_organizations_type CHECK (
        organization_type IN ('foundation', 'standards_body', 'nonprofit', 'government', 'university', 'community', 'other')
    ),
    CONSTRAINT chk_organizations_status CHECK (
        status IN ('active', 'inactive', 'unknown')
    ),
    CONSTRAINT chk_organizations_metadata_object   CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT chk_organizations_governance_object CHECK (jsonb_typeof(governance) = 'object'),
    CONSTRAINT chk_organizations_evidence_object   CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE INDEX idx_organizations_display_name ON organizations(lower(display_name));

CREATE TABLE open_source_projects (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_slug   TEXT NOT NULL UNIQUE,
    display_name     TEXT NOT NULL,
    website          TEXT,
    repository_url   TEXT,
    license          TEXT,
    short_description TEXT,
    description      TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'active',
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT NOT NULL DEFAULT 'active',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_open_source_projects_lifecycle_status CHECK (
        lifecycle_status IN ('active', 'maintenance', 'deprecated', 'unknown')
    ),
    CONSTRAINT chk_open_source_projects_status CHECK (
        status IN ('active', 'inactive', 'unknown')
    ),
    CONSTRAINT chk_open_source_projects_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT chk_open_source_projects_evidence_object CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE INDEX idx_open_source_projects_display_name ON open_source_projects(lower(display_name));

GRANT SELECT ON organizations        TO corpscout_anon;
GRANT SELECT ON open_source_projects TO corpscout_anon;
