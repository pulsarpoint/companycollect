CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.no_financial_statements
    ADD COLUMN IF NOT EXISTS quality_flag LowCardinality(String) DEFAULT '' AFTER accounting_rules;

-- One-time backfill for rows exported before the normalize step set the flag.
-- Mirrors _quality_flag in norway_brreg_financial/financial_normalize.py:
-- small-enterprise filings above 500bn (in filing currency) are implausible
-- magnitudes (live case: org 983096077 filed 2022 accounts inflated x1e6 at
-- the source -- Brreg serves 13.9 trillion NOK revenue for a small nonprofit).
ALTER TABLE corpscout.no_financial_statements
    UPDATE quality_flag = 'implausible_magnitude'
    WHERE is_small_enterprise = 1
      AND (operating_revenue_amount_original > 500000000000
        OR total_assets_amount_original > 500000000000)
    SETTINGS mutations_sync = 2;
