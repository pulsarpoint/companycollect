-- Extend companies with enrichment fields mirroring backoffice_v2 company_profiles
ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS short_name        TEXT,
    ADD COLUMN IF NOT EXISTS short_description TEXT,
    ADD COLUMN IF NOT EXISTS description       TEXT,
    ADD COLUMN IF NOT EXISTS website           TEXT,
    ADD COLUMN IF NOT EXISTS founded_year      INTEGER,
    ADD COLUMN IF NOT EXISTS employee_estimate JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS revenue_estimate  JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS ownership         JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE companies
    ADD CONSTRAINT chk_companies_founded_year
        CHECK (founded_year IS NULL OR founded_year BETWEEN 1000 AND 3000),
    ADD CONSTRAINT chk_companies_employee_estimate_object
        CHECK (jsonb_typeof(employee_estimate) = 'object'),
    ADD CONSTRAINT chk_companies_revenue_estimate_object
        CHECK (jsonb_typeof(revenue_estimate) = 'object'),
    ADD CONSTRAINT chk_companies_ownership_object
        CHECK (jsonb_typeof(ownership) = 'object');

-- ── company_locations ────────────────────────────────────────────────────────
CREATE TABLE company_locations (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id     UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    location_type  TEXT NOT NULL DEFAULT 'headquarters',
    label          TEXT,
    address_line1  TEXT,
    address_line2  TEXT,
    city           TEXT,
    region         TEXT,
    postal_code    TEXT,
    country        TEXT,
    country_code   TEXT,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    geo_metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,
    source         TEXT NOT NULL DEFAULT 'registry',
    confidence     REAL,
    evidence       JSONB NOT NULL DEFAULT '{}'::jsonb,
    removed_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_locations_type
        CHECK (location_type IN ('headquarters', 'registered_address', 'office')),
    CONSTRAINT chk_company_locations_country_code
        CHECK (country_code IS NULL OR country_code = upper(country_code)),
    CONSTRAINT chk_company_locations_latitude
        CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CONSTRAINT chk_company_locations_longitude
        CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    CONSTRAINT chk_company_locations_geo_metadata_object
        CHECK (jsonb_typeof(geo_metadata) = 'object'),
    CONSTRAINT chk_company_locations_evidence_object
        CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_company_locations_confidence
        CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
);

CREATE UNIQUE INDEX uq_company_locations_active_headquarters
    ON company_locations(company_id, location_type)
    WHERE removed_at IS NULL AND location_type = 'headquarters';

CREATE INDEX idx_company_locations_company
    ON company_locations(company_id, removed_at, location_type);

-- ── company_phones ────────────────────────────────────────────────────────────
CREATE TABLE company_phones (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    phone       TEXT NOT NULL,
    description TEXT,
    purpose     TEXT NOT NULL DEFAULT 'general',
    source      TEXT NOT NULL DEFAULT 'registry',
    confidence  REAL,
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
    removed_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_phones_phone
        CHECK (btrim(phone) <> ''),
    CONSTRAINT chk_company_phones_purpose
        CHECK (purpose IN ('main', 'support', 'sales', 'security', 'general')),
    CONSTRAINT chk_company_phones_confidence
        CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
    CONSTRAINT chk_company_phones_evidence_object
        CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_company_phones_metadata_object
        CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_company_phones_active
    ON company_phones(company_id, phone, purpose)
    WHERE removed_at IS NULL;

-- ── company_emails ────────────────────────────────────────────────────────────
CREATE TABLE company_emails (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    email       TEXT NOT NULL,
    description TEXT,
    purpose     TEXT NOT NULL DEFAULT 'general',
    name        TEXT,
    source      TEXT NOT NULL DEFAULT 'registry',
    confidence  REAL,
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
    removed_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_emails_email
        CHECK (btrim(email) <> ''),
    CONSTRAINT chk_company_emails_purpose
        CHECK (purpose IN ('support', 'security', 'abuse', 'sales', 'privacy', 'general')),
    CONSTRAINT chk_company_emails_confidence
        CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
    CONSTRAINT chk_company_emails_evidence_object
        CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_company_emails_metadata_object
        CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_company_emails_active
    ON company_emails(company_id, lower(email), purpose)
    WHERE removed_at IS NULL;

-- ── company_industries ────────────────────────────────────────────────────────
CREATE TABLE company_industries (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    industry    TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'registry',
    confidence  REAL,
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_industries_value
        CHECK (btrim(industry) <> ''),
    CONSTRAINT chk_company_industries_confidence
        CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
    CONSTRAINT uq_company_industries UNIQUE (company_id, industry)
);

-- ── company_markets ───────────────────────────────────────────────────────────
CREATE TABLE company_markets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    market      TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'registry',
    confidence  REAL,
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_markets_value
        CHECK (btrim(market) <> ''),
    CONSTRAINT chk_company_markets_confidence
        CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
    CONSTRAINT uq_company_markets UNIQUE (company_id, market)
);

-- ── company_services ──────────────────────────────────────────────────────────
CREATE TABLE company_services (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    service     TEXT NOT NULL,
    description TEXT,
    source      TEXT NOT NULL DEFAULT 'registry',
    confidence  REAL,
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_services_value
        CHECK (btrim(service) <> ''),
    CONSTRAINT chk_company_services_confidence
        CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
    CONSTRAINT uq_company_services UNIQUE (company_id, service)
);

-- ── Refresh v_companies to include new enrichment fields ──────────────────────
DROP VIEW IF EXISTS v_companies;
CREATE VIEW v_companies AS
SELECT
    c.id,
    c.name,
    c.short_name,
    c.registration_number,
    c.lei,
    c.status,
    c.website,
    c.short_description,
    c.founded_year,
    c.employee_estimate,
    c.revenue_estimate,
    c.ownership,
    c.created_at,
    c.updated_at,
    co.id           AS country_id,
    co.name         AS country_name,
    co.iso_alpha2   AS country_iso2,
    ds.name         AS primary_source,
    ds.display_name AS primary_source_display_name,
    (SELECT COUNT(*)::int FROM company_domains cd WHERE cd.company_id = c.id) AS domain_count,
    (
        SELECT cl.city || COALESCE(', ' || cl.country_code, '')
        FROM company_locations cl
        WHERE cl.company_id = c.id AND cl.removed_at IS NULL AND cl.location_type = 'headquarters'
        LIMIT 1
    ) AS headquarters_location
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;

-- ── Sub-table views ───────────────────────────────────────────────────────────
CREATE VIEW v_company_locations AS
SELECT cl.*
FROM company_locations cl
WHERE cl.removed_at IS NULL;

CREATE VIEW v_company_phones AS
SELECT cp.*
FROM company_phones cp
WHERE cp.removed_at IS NULL;

CREATE VIEW v_company_emails AS
SELECT ce.*
FROM company_emails ce
WHERE ce.removed_at IS NULL;

CREATE VIEW v_company_industries AS
SELECT ci.*
FROM company_industries ci;

CREATE VIEW v_company_markets AS
SELECT cm.*
FROM company_markets cm;

CREATE VIEW v_company_services AS
SELECT cs.*
FROM company_services cs;

-- Grant SELECT on new tables and views to anon role
GRANT SELECT ON company_locations    TO corpscout_anon;
GRANT SELECT ON company_phones       TO corpscout_anon;
GRANT SELECT ON company_emails       TO corpscout_anon;
GRANT SELECT ON company_industries   TO corpscout_anon;
GRANT SELECT ON company_markets      TO corpscout_anon;
GRANT SELECT ON company_services     TO corpscout_anon;
GRANT SELECT ON v_company_locations  TO corpscout_anon;
GRANT SELECT ON v_company_phones     TO corpscout_anon;
GRANT SELECT ON v_company_emails     TO corpscout_anon;
GRANT SELECT ON v_company_industries TO corpscout_anon;
GRANT SELECT ON v_company_markets    TO corpscout_anon;
GRANT SELECT ON v_company_services   TO corpscout_anon;
