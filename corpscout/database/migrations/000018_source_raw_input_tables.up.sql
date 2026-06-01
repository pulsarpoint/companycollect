-- GLEIF raw inputs.
CREATE TABLE gleif_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    lei TEXT NOT NULL,
    legal_name TEXT,
    registration_status TEXT,
    headquarters_country_code TEXT,
    parent_lei TEXT,
    ultimate_parent_lei TEXT,
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_gleif_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_gleif_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_gleif_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_gleif_company_raw_inputs_payload UNIQUE (lei, payload_hash)
);

CREATE INDEX idx_gleif_raw_processing
    ON gleif_company_raw_inputs(processing_status, processing_lease_until, created_at);

-- Companies House raw inputs.
CREATE TABLE companies_house_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    company_number TEXT NOT NULL,
    company_name TEXT,
    company_status TEXT,
    company_type TEXT,
    country_iso2 TEXT DEFAULT 'GB',
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_ch_raw_source_native CHECK (source_native_id = company_number),
    CONSTRAINT chk_ch_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_ch_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_ch_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_companies_house_raw_payload UNIQUE (company_number, payload_hash)
);

CREATE INDEX idx_ch_raw_processing
    ON companies_house_company_raw_inputs(processing_status, processing_lease_until, created_at);
CREATE INDEX idx_ch_raw_company_number
    ON companies_house_company_raw_inputs(company_number);

-- Brreg raw inputs.
CREATE TABLE brreg_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    organization_number TEXT NOT NULL,
    organization_name TEXT,
    registration_status TEXT,
    website TEXT,
    country_iso2 TEXT DEFAULT 'NO',
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_brreg_raw_source_native CHECK (source_native_id = organization_number),
    CONSTRAINT chk_brreg_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_brreg_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_brreg_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_brreg_raw_payload UNIQUE (organization_number, payload_hash)
);

CREATE INDEX idx_brreg_raw_processing
    ON brreg_company_raw_inputs(processing_status, processing_lease_until, created_at);
CREATE INDEX idx_brreg_raw_organization_number
    ON brreg_company_raw_inputs(organization_number);

-- AI company profile raw inputs.
CREATE TABLE ai_company_profile_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    normalized_website TEXT,
    normalized_domain TEXT,
    requested_company_name TEXT,
    model_name TEXT,
    prompt_version TEXT,
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_ai_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_ai_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_ai_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_ai_company_profile_raw_payload UNIQUE (normalized_domain, prompt_version, payload_hash)
);

CREATE INDEX idx_ai_raw_processing
    ON ai_company_profile_raw_inputs(processing_status, processing_lease_until, created_at);

-- Domain discovery raw inputs (processor deferred, table added for completeness).
CREATE TABLE domain_discovery_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    domain TEXT NOT NULL,
    signal TEXT,
    confidence REAL,
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_domain_discovery_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_domain_discovery_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_domain_discovery_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT chk_domain_discovery_raw_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    )
);

CREATE INDEX idx_domain_discovery_raw_processing
    ON domain_discovery_raw_inputs(processing_status, processing_lease_until, created_at);
