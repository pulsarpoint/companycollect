ALTER TABLE data_sources
    ADD CONSTRAINT uq_data_sources_id_name UNIQUE (id, name);

CREATE TABLE suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    created_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    source_id UUID NOT NULL,
    source_type TEXT NOT NULL,
    source_input_table TEXT NOT NULL,
    source_input_id TEXT NOT NULL,
    source_native_id TEXT,
    source_payload_hash TEXT,
    source_pull_run_id UUID REFERENCES source_pull_runs(id) ON DELETE SET NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_suggestions_source_type
        FOREIGN KEY (source_id, source_type) REFERENCES data_sources(id, name),
    CONSTRAINT chk_suggestions_target CHECK (
        NOT (target_company_id IS NOT NULL AND created_company_id IS NOT NULL)
    ),
    CONSTRAINT chk_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestions_status CHECK (
        status IN ('pending', 'partially_applied', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT uq_suggestions_source_input UNIQUE (
        source_input_table, source_input_id, source_payload_hash
    )
);

CREATE INDEX idx_suggestions_review ON suggestions(status, created_at DESC);
CREATE INDEX idx_suggestions_target_company
    ON suggestions(target_company_id, status)
    WHERE target_company_id IS NOT NULL;
CREATE INDEX idx_suggestions_created_company
    ON suggestions(created_company_id, status)
    WHERE created_company_id IS NOT NULL;
CREATE INDEX idx_suggestions_source
    ON suggestions(source_id, source_input_table, source_input_id);

CREATE TABLE suggestion_company_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    name TEXT,
    display_name TEXT,
    canonical_slug TEXT,
    country_id UUID REFERENCES countries(id) ON DELETE RESTRICT,
    registration_number TEXT,
    lei VARCHAR(20),
    lifecycle_status TEXT,
    website TEXT,
    short_name TEXT,
    short_description TEXT,
    description TEXT,
    founded_year INTEGER,
    employee_count INTEGER,
    revenue_usd BIGINT,
    parent_lei VARCHAR(20),
    ultimate_parent_lei VARCHAR(20),
    employee_estimate JSONB NOT NULL DEFAULT '{}'::jsonb,
    revenue_estimate JSONB NOT NULL DEFAULT '{}'::jsonb,
    ownership JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_profiles_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_profiles_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_profiles_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_profiles_founded_year CHECK (
        founded_year IS NULL OR founded_year BETWEEN 1000 AND 3000
    ),
    CONSTRAINT chk_suggestion_company_profiles_employee_estimate CHECK (
        jsonb_typeof(employee_estimate) = 'object'
    ),
    CONSTRAINT chk_suggestion_company_profiles_revenue_estimate CHECK (
        jsonb_typeof(revenue_estimate) = 'object'
    ),
    CONSTRAINT chk_suggestion_company_profiles_ownership CHECK (
        jsonb_typeof(ownership) = 'object'
    ),
    CONSTRAINT chk_suggestion_company_profiles_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE UNIQUE INDEX uq_suggestion_company_profiles_pending
    ON suggestion_company_profiles(suggestion_id)
    WHERE status = 'pending';

CREATE TABLE suggestion_company_domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_domains(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    domain TEXT NOT NULL,
    relationship_type TEXT NOT NULL DEFAULT 'candidate',
    domain_status TEXT NOT NULL DEFAULT 'needs_review',
    signal TEXT NOT NULL,
    signal_confidence SMALLINT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_domains_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_domains_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_domains_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_domains_relationship CHECK (
        relationship_type IN ('official_site', 'brand', 'subsidiary', 'old_domain', 'candidate')
    ),
    CONSTRAINT chk_suggestion_company_domains_domain_status CHECK (
        domain_status IN ('active', 'needs_review', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_domains_signal CHECK (
        signal IN ('registry_website', 'registry_email_domain', 'wikidata', 'certsh', 'whois', 'search', 'manual_import')
    ),
    CONSTRAINT chk_suggestion_company_domains_signal_confidence CHECK (
        signal_confidence BETWEEN 1 AND 100
    ),
    CONSTRAINT chk_suggestion_company_domains_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_domains_review
    ON suggestion_company_domains(suggestion_id, status);

CREATE TABLE suggestion_company_locations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_locations(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    location_type TEXT NOT NULL DEFAULT 'headquarters',
    label TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    region TEXT,
    postal_code TEXT,
    country TEXT,
    country_code TEXT,
    country_id UUID REFERENCES countries(id) ON DELETE SET NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geo_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'registry',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_locations_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_locations_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_locations_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_locations_type CHECK (
        location_type IN ('headquarters', 'registered_address', 'office')
    ),
    CONSTRAINT chk_suggestion_company_locations_country_code CHECK (
        country_code IS NULL OR country_code = upper(country_code)
    ),
    CONSTRAINT chk_suggestion_company_locations_latitude CHECK (
        latitude IS NULL OR latitude BETWEEN -90 AND 90
    ),
    CONSTRAINT chk_suggestion_company_locations_longitude CHECK (
        longitude IS NULL OR longitude BETWEEN -180 AND 180
    ),
    CONSTRAINT chk_suggestion_company_locations_geo_metadata CHECK (
        jsonb_typeof(geo_metadata) = 'object'
    ),
    CONSTRAINT chk_suggestion_company_locations_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_locations_review
    ON suggestion_company_locations(suggestion_id, status);

CREATE TABLE suggestion_company_emails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_emails(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    email TEXT NOT NULL,
    description TEXT,
    purpose TEXT NOT NULL DEFAULT 'general',
    name TEXT,
    source TEXT NOT NULL DEFAULT 'registry',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_emails_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_emails_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_emails_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_emails_value CHECK (btrim(email) <> ''),
    CONSTRAINT chk_suggestion_company_emails_purpose CHECK (
        purpose IN ('support', 'security', 'abuse', 'sales', 'privacy', 'general')
    ),
    CONSTRAINT chk_suggestion_company_emails_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    ),
    CONSTRAINT chk_suggestion_company_emails_metadata CHECK (
        jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_emails_review
    ON suggestion_company_emails(suggestion_id, status);

CREATE TABLE suggestion_company_phones (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_phones(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    phone TEXT NOT NULL,
    description TEXT,
    purpose TEXT NOT NULL DEFAULT 'general',
    source TEXT NOT NULL DEFAULT 'registry',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_phones_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_phones_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_phones_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_phones_value CHECK (btrim(phone) <> ''),
    CONSTRAINT chk_suggestion_company_phones_purpose CHECK (
        purpose IN ('main', 'support', 'sales', 'security', 'general')
    ),
    CONSTRAINT chk_suggestion_company_phones_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    ),
    CONSTRAINT chk_suggestion_company_phones_metadata CHECK (
        jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_phones_review
    ON suggestion_company_phones(suggestion_id, status);

CREATE TABLE suggestion_company_financials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_financials(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    year INT NOT NULL,
    source_name TEXT NOT NULL,
    employee_count INT,
    revenue_amount BIGINT,
    revenue_currency TEXT,
    revenue_usd BIGINT,
    profit_amount BIGINT,
    profit_usd BIGINT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_financials_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_financials_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_financials_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_financials_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_financials_review
    ON suggestion_company_financials(suggestion_id, status);

CREATE TABLE suggestion_company_industries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_industries(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    industry TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'registry',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_industries_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_industries_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_industries_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_industries_value CHECK (btrim(industry) <> ''),
    CONSTRAINT chk_suggestion_company_industries_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_industries_review
    ON suggestion_company_industries(suggestion_id, status);

CREATE TABLE suggestion_company_markets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_markets(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    market TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'registry',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_markets_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_markets_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_markets_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_markets_value CHECK (btrim(market) <> ''),
    CONSTRAINT chk_suggestion_company_markets_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_markets_review
    ON suggestion_company_markets(suggestion_id, status);

CREATE TABLE suggestion_company_services (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_services(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    service TEXT NOT NULL,
    description TEXT,
    source TEXT NOT NULL DEFAULT 'registry',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_services_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_services_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_services_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_services_value CHECK (btrim(service) <> ''),
    CONSTRAINT chk_suggestion_company_services_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_services_review
    ON suggestion_company_services(suggestion_id, status);

CREATE TABLE suggestion_company_relationships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_id UUID NOT NULL REFERENCES suggestions(id) ON DELETE CASCADE,
    target_row_id UUID REFERENCES company_relationships(id) ON DELETE SET NULL,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    related_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    related_company_name TEXT,
    related_lei VARCHAR(20),
    relationship_type TEXT NOT NULL,
    ownership_percentage NUMERIC(5,2),
    valid_from DATE,
    valid_to DATE,
    source TEXT NOT NULL DEFAULT 'registry',
    relationship_status TEXT NOT NULL DEFAULT 'needs_review',
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_company_relationships_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_suggestion_company_relationships_status CHECK (
        status IN ('pending', 'applied', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_relationships_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_suggestion_company_relationships_type CHECK (
        relationship_type IN (
            'direct_parent', 'ultimate_parent', 'subsidiary_of',
            'owned_by', 'acquired_by', 'merged_into', 'same_group', 'other'
        )
    ),
    CONSTRAINT chk_suggestion_company_relationships_ownership CHECK (
        ownership_percentage IS NULL OR ownership_percentage BETWEEN 0 AND 100
    ),
    CONSTRAINT chk_suggestion_company_relationships_relation_status CHECK (
        relationship_status IN ('active', 'needs_review', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_suggestion_company_relationships_valid_range CHECK (
        valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from
    ),
    CONSTRAINT chk_suggestion_company_relationships_evidence CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX idx_suggestion_company_relationships_review
    ON suggestion_company_relationships(suggestion_id, status);
