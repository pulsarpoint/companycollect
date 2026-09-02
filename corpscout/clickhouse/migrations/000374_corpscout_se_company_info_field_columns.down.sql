CREATE DATABASE IF NOT EXISTS corpscout;

-- No MATERIALIZED expression reads any of these (evidence_set_hash covers
-- evidence_hashes only), so they drop cleanly, newest first.

ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS latest_revenue_fiscal_year,
    DROP COLUMN IF EXISTS latest_revenue_amount_usd,
    DROP COLUMN IF EXISTS latest_revenue_currency,
    DROP COLUMN IF EXISTS latest_revenue_amount,
    DROP COLUMN IF EXISTS employee_count_as_of,
    DROP COLUMN IF EXISTS employee_count,
    DROP COLUMN IF EXISTS website,
    DROP COLUMN IF EXISTS industry_label_en;
