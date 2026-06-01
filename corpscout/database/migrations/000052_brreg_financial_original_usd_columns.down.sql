DROP INDEX IF EXISTS idx_brreg_source_financials_revenue_usd;

ALTER TABLE brreg_source_financials
  DROP CONSTRAINT IF EXISTS chk_brreg_source_financials_fx_metadata_object;

ALTER TABLE brreg_source_financials
  DROP COLUMN IF EXISTS fx_source,
  DROP COLUMN IF EXISTS fx_rate_date,
  DROP COLUMN IF EXISTS fx_metadata,
  DROP COLUMN IF EXISTS revenue_usd_cents,
  DROP COLUMN IF EXISTS operating_profit_usd_cents,
  DROP COLUMN IF EXISTS profit_before_tax_usd_cents,
  DROP COLUMN IF EXISTS net_income_usd_cents,
  DROP COLUMN IF EXISTS total_assets_usd_cents,
  DROP COLUMN IF EXISTS total_equity_usd_cents,
  DROP COLUMN IF EXISTS total_liabilities_usd_cents;

ALTER INDEX idx_brreg_source_financials_revenue_original
  RENAME TO idx_brreg_source_financials_revenue;

ALTER TABLE brreg_source_financials
  RENAME COLUMN original_currency TO currency;

ALTER TABLE brreg_source_financials
  RENAME COLUMN revenue_original_amount TO revenue_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN operating_profit_original_amount TO operating_profit_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN profit_before_tax_original_amount TO profit_before_tax_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN net_income_original_amount TO net_income_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN total_assets_original_amount TO total_assets_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN total_equity_original_amount TO total_equity_amount;

ALTER TABLE brreg_source_financials
  RENAME COLUMN total_liabilities_original_amount TO total_liabilities_amount;
