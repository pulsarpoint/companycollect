-- ── v_companies: drop before altering companies columns ──────────────────────
DROP VIEW IF EXISTS v_companies;

-- ── company_financials: per-year financial records with suggestion workflow ────
CREATE TABLE IF NOT EXISTS company_financials (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    year            INT NOT NULL,
    source_name     TEXT NOT NULL,
    employee_count  INT,
    revenue_amount  BIGINT,        -- original currency, cents
    revenue_currency TEXT,         -- ISO 4217
    revenue_usd     BIGINT,        -- USD cents
    profit_amount   BIGINT,        -- original currency, cents
    profit_usd      BIGINT,        -- USD cents
    status          TEXT NOT NULL DEFAULT 'suggested'
                        CHECK (status IN ('suggested', 'approved', 'rejected')),
    reviewed_by     TEXT,
    reviewed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_company_financials_company_id
    ON company_financials (company_id);
CREATE INDEX IF NOT EXISTS idx_company_financials_status
    ON company_financials (status) WHERE status = 'suggested';
CREATE UNIQUE INDEX IF NOT EXISTS idx_company_financials_company_year_source
    ON company_financials (company_id, year, source_name);

-- ── companies: remove columns that belong in company_financials ───────────────
ALTER TABLE companies
    DROP COLUMN IF EXISTS profit_usd,
    DROP COLUMN IF EXISTS revenue_orig_amount,
    DROP COLUMN IF EXISTS revenue_orig_currency,
    DROP COLUMN IF EXISTS profit_estimate;

-- ── v_companies: rebuild without removed columns ──────────────────────────────
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
    c.description,
    c.founded_year,
    c.employee_estimate,
    c.revenue_estimate,
    c.ownership,
    c.employee_count,
    c.revenue_usd,
    c.created_at,
    c.updated_at,
    co.id           AS country_id,
    co.name         AS country_name,
    co.iso_alpha2   AS country_iso2,
    ds.name         AS primary_source,
    ds.display_name AS primary_source_display_name,
    (SELECT COUNT(*)::int FROM company_domains cd WHERE cd.company_id = c.id) AS domain_count,
    (
        SELECT NULLIF(TRIM(CONCAT_WS(', ', cl.city, cl.region, cl.country)), '')
        FROM company_locations cl
        WHERE cl.company_id = c.id
          AND cl.location_type = 'headquarters'
        LIMIT 1
    ) AS headquarters_location
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;
