ALTER TABLE brreg_source_financials
  RENAME COLUMN currency TO original_currency;

ALTER TABLE brreg_source_financials
  RENAME COLUMN revenue_amount TO revenue_original_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN operating_profit_amount TO operating_profit_original_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN profit_before_tax_amount TO profit_before_tax_original_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN net_income_amount TO net_income_original_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN total_assets_amount TO total_assets_original_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN total_equity_amount TO total_equity_original_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN total_liabilities_amount TO total_liabilities_original_amount;

ALTER INDEX idx_brreg_source_financials_revenue
  RENAME TO idx_brreg_source_financials_revenue_original;

ALTER TABLE brreg_source_financials
  ADD COLUMN fx_source TEXT,
  ADD COLUMN fx_rate_date DATE,
  ADD COLUMN fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN revenue_usd_cents BIGINT,
  ADD COLUMN operating_profit_usd_cents BIGINT,
  ADD COLUMN profit_before_tax_usd_cents BIGINT,
  ADD COLUMN net_income_usd_cents BIGINT,
  ADD COLUMN total_assets_usd_cents BIGINT,
  ADD COLUMN total_equity_usd_cents BIGINT,
  ADD COLUMN total_liabilities_usd_cents BIGINT,
  ADD CONSTRAINT chk_brreg_source_financials_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object');

CREATE INDEX idx_brreg_source_financials_revenue_usd
  ON brreg_source_financials(revenue_usd_cents DESC)
  WHERE revenue_usd_cents IS NOT NULL;
