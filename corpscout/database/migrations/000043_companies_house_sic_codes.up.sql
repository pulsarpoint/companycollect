CREATE TABLE companies_house_sic_codes (
    code TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    section_code TEXT,
    section_description TEXT,
    source_url TEXT NOT NULL,
    source_sha256 TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_companies_house_sic_codes_code CHECK (code ~ '^[0-9]{5}$'),
    CONSTRAINT chk_companies_house_sic_codes_description CHECK (btrim(description) <> ''),
    CONSTRAINT chk_companies_house_sic_codes_source_url CHECK (btrim(source_url) <> '')
);

CREATE INDEX idx_companies_house_sic_codes_description
    ON companies_house_sic_codes USING gin (to_tsvector('english', description));

GRANT SELECT ON companies_house_sic_codes TO corpscout_anon;
