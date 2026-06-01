-- ── companies: new financial columns ─────────────────────────────────────────
ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS profit_estimate      JSONB,
  ADD COLUMN IF NOT EXISTS employee_count       INT,
  ADD COLUMN IF NOT EXISTS revenue_usd          BIGINT,
  ADD COLUMN IF NOT EXISTS revenue_orig_amount  BIGINT,
  ADD COLUMN IF NOT EXISTS revenue_orig_currency TEXT,
  ADD COLUMN IF NOT EXISTS profit_usd           BIGINT;

CREATE INDEX IF NOT EXISTS idx_companies_employee_count
  ON companies (employee_count) WHERE employee_count IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_companies_revenue_usd
  ON companies (revenue_usd) WHERE revenue_usd IS NOT NULL;

-- ── data_sources: country_id + capabilities ───────────────────────────────────
ALTER TABLE data_sources
  ADD COLUMN IF NOT EXISTS country_id    UUID REFERENCES countries(id),
  ADD COLUMN IF NOT EXISTS capabilities  TEXT[] NOT NULL DEFAULT '{}';

-- Seed country_id for country-specific sources
UPDATE data_sources SET country_id = (SELECT id FROM countries WHERE iso_alpha2 = 'NO')
  WHERE name = 'brreg';
UPDATE data_sources SET country_id = (SELECT id FROM countries WHERE iso_alpha2 = 'GB')
  WHERE name = 'companies_house';
UPDATE data_sources SET country_id = (SELECT id FROM countries WHERE iso_alpha2 = 'DK')
  WHERE name = 'cvr';
UPDATE data_sources SET country_id = (SELECT id FROM countries WHERE iso_alpha2 = 'EE')
  WHERE name = 'ariregister';

-- Seed capabilities
UPDATE data_sources SET capabilities = '{employee_count,revenue,profit,company_name,org_number}'
  WHERE name = 'brreg';
UPDATE data_sources SET capabilities = '{employee_count,company_name,status,directors}'
  WHERE name = 'companies_house';
UPDATE data_sources SET capabilities = '{company_name,lei,legal_form,status,locations}'
  WHERE name = 'gleif';

-- ── v_companies: add new scalar columns ──────────────────────────────────────
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
    c.description,
    c.founded_year,
    c.employee_estimate,
    c.revenue_estimate,
    c.profit_estimate,
    c.ownership,
    c.employee_count,
    c.revenue_usd,
    c.revenue_orig_amount,
    c.revenue_orig_currency,
    c.profit_usd,
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
