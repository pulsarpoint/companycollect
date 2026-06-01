CREATE TABLE company_addresses (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    native_id      TEXT NOT NULL,
    source         TEXT NOT NULL,
    address_line_1 TEXT,
    address_line_2 TEXT,
    locality       TEXT,
    postal_code    TEXT,
    country        TEXT,
    region         TEXT,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (native_id, source)
);

CREATE INDEX idx_company_addresses_native_id ON company_addresses (native_id);
