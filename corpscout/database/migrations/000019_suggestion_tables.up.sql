-- Provenance glue between source inputs and suggestions.
CREATE TABLE suggestion_source_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_table TEXT NOT NULL,
    suggestion_id UUID NOT NULL,
    source_id UUID NOT NULL REFERENCES data_sources(id),
    source_input_table TEXT NOT NULL,
    source_input_key TEXT NOT NULL,
    source_pull_run_id UUID REFERENCES source_pull_runs(id),
    confidence REAL,
    evidence_excerpt TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_source_links_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    )
);

CREATE INDEX idx_suggestion_source_links_suggestion
    ON suggestion_source_links(suggestion_table, suggestion_id);
CREATE INDEX idx_suggestion_source_links_source
    ON suggestion_source_links(source_id, source_input_table, source_input_key);

-- Root suggestion: proposed new company.
-- proposed_country_id is required at approval time because companies.country_id is NOT NULL.
-- Processors must supply it from the source record (e.g. GLEIF headquarters_country_code,
-- Companies House GB, Brreg NO).
CREATE TABLE company_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_legal_name TEXT,
    proposed_website TEXT,
    proposed_canonical_slug TEXT,
    proposed_country_id UUID REFERENCES countries(id),
    proposed_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_company_suggestions_created_company_when_approved CHECK (
        status <> 'approved' OR created_company_id IS NOT NULL
    )
);

CREATE INDEX idx_company_suggestions_review
    ON company_suggestions(status, proposed_display_name);

-- Root suggestion: proposed new organization.
CREATE TABLE organization_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_organization_type TEXT NOT NULL,
    proposed_website TEXT,
    proposed_canonical_slug TEXT,
    proposed_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_organization_suggestions_type CHECK (
        proposed_organization_type IN (
            'foundation', 'standards_body', 'nonprofit',
            'government', 'university', 'community', 'other'
        )
    ),
    CONSTRAINT chk_organization_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_organization_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_organization_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_organization_suggestions_created_org_when_approved CHECK (
        status <> 'approved' OR created_organization_id IS NOT NULL
    )
);

CREATE INDEX idx_organization_suggestions_review
    ON organization_suggestions(status, proposed_display_name);

-- Root suggestion: proposed new open-source project.
CREATE TABLE open_source_project_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_repository_url TEXT,
    proposed_website TEXT,
    proposed_license TEXT,
    proposed_lifecycle_status TEXT,
    proposed_canonical_slug TEXT,
    proposed_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE SET NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_osp_suggestions_lifecycle CHECK (
        proposed_lifecycle_status IS NULL
        OR proposed_lifecycle_status IN ('active', 'maintenance', 'deprecated', 'unknown')
    ),
    CONSTRAINT chk_osp_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_osp_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_osp_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_osp_suggestions_created_project_when_approved CHECK (
        status <> 'approved' OR created_open_source_project_id IS NOT NULL
    )
);

CREATE INDEX idx_osp_suggestions_review
    ON open_source_project_suggestions(status, proposed_display_name);

-- Section suggestion: domain changes for companies.
CREATE TABLE company_domain_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    domain TEXT NOT NULL,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_domain_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_domain_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_domain_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_domain_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_domain_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_domain_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_domain_suggestions_existing
    ON company_domain_suggestions(company_id, status)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_domain_suggestions_new
    ON company_domain_suggestions(company_suggestion_id, status)
    WHERE company_suggestion_id IS NOT NULL;

-- Section suggestion: contact changes (email, phone, website, social) for companies.
CREATE TABLE company_contact_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    contact_kind TEXT NOT NULL,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_contact_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_contact_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_contact_suggestions_contact_kind CHECK (
        contact_kind IN ('email', 'phone', 'website', 'social', 'other')
    ),
    CONSTRAINT chk_company_contact_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_contact_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_contact_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_contact_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

-- Section suggestion: address/location changes for companies.
CREATE TABLE company_location_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    location_kind TEXT NOT NULL,
    country_code TEXT,
    city TEXT,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_location_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_location_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_location_suggestions_location_kind CHECK (
        location_kind IN ('headquarters', 'registered', 'office', 'branch', 'other')
    ),
    CONSTRAINT chk_company_location_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_location_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_location_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_location_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_location_suggestions_existing
    ON company_location_suggestions(company_id, status)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_location_suggestions_new
    ON company_location_suggestions(company_suggestion_id, status)
    WHERE company_suggestion_id IS NOT NULL;

-- Section suggestion: scalar lifecycle/status/registry field changes for companies.
CREATE TABLE company_status_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    status_field TEXT NOT NULL,
    current_value TEXT,
    proposed_value TEXT,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_status_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_status_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_status_suggestions_status_field CHECK (
        status_field IN (
            'lifecycle_status', 'registration_status', 'legal_name',
            'registration_number', 'lei', 'other'
        )
    ),
    CONSTRAINT chk_company_status_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_status_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_status_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_status_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_status_suggestions_existing
    ON company_status_suggestions(company_id, status, status_field)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_status_suggestions_new
    ON company_status_suggestions(company_suggestion_id, status, status_field)
    WHERE company_suggestion_id IS NOT NULL;

-- Section suggestion: parent/subsidiary/ownership relationship changes for companies.
CREATE TABLE company_relationship_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    related_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    related_company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE SET NULL,
    related_company_name TEXT,
    related_lei TEXT,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_relationship_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_relationship_suggestions_related_target CHECK (
        NOT (related_company_id IS NOT NULL AND related_company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_relationship_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_relationship_suggestions_relationship_type CHECK (
        relationship_type IN (
            'direct_parent', 'ultimate_parent', 'subsidiary_of',
            'owned_by', 'acquired_by', 'merged_into', 'other'
        )
    ),
    CONSTRAINT chk_company_relationship_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_relationship_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_relationship_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_relationship_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_relationship_suggestions_existing
    ON company_relationship_suggestions(company_id, status, relationship_type)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_relationship_suggestions_new
    ON company_relationship_suggestions(company_suggestion_id, status, relationship_type)
    WHERE company_suggestion_id IS NOT NULL;
