package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregFinancialUSDColumnsMigrationRenamesOriginalAmounts(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000052_brreg_financial_original_usd_columns.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "RENAME COLUMN currency TO original_currency")
	require.Contains(t, sql, "RENAME COLUMN revenue_amount TO revenue_original_amount")
	require.Contains(t, sql, "RENAME COLUMN operating_profit_amount TO operating_profit_original_amount")
	require.Contains(t, sql, "RENAME COLUMN profit_before_tax_amount TO profit_before_tax_original_amount")
	require.Contains(t, sql, "RENAME COLUMN net_income_amount TO net_income_original_amount")
	require.Contains(t, sql, "RENAME COLUMN total_assets_amount TO total_assets_original_amount")
	require.Contains(t, sql, "RENAME COLUMN total_equity_amount TO total_equity_original_amount")
	require.Contains(t, sql, "RENAME COLUMN total_liabilities_amount TO total_liabilities_original_amount")
}

func TestBrregFinancialUSDColumnsMigrationAddsUSDAndFXColumns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000052_brreg_financial_original_usd_columns.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "ADD COLUMN fx_source TEXT")
	require.Contains(t, sql, "ADD COLUMN fx_rate_date DATE")
	require.Contains(t, sql, "ADD COLUMN fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "ADD COLUMN revenue_usd_cents BIGINT")
	require.Contains(t, sql, "ADD COLUMN operating_profit_usd_cents BIGINT")
	require.Contains(t, sql, "ADD COLUMN profit_before_tax_usd_cents BIGINT")
	require.Contains(t, sql, "ADD COLUMN net_income_usd_cents BIGINT")
	require.Contains(t, sql, "ADD COLUMN total_assets_usd_cents BIGINT")
	require.Contains(t, sql, "ADD COLUMN total_equity_usd_cents BIGINT")
	require.Contains(t, sql, "ADD COLUMN total_liabilities_usd_cents BIGINT")
	require.Contains(t, sql, "CONSTRAINT chk_brreg_source_financials_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object')")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_source_financials_revenue_usd")
	require.Contains(t, sql, "ON brreg_source_financials(revenue_usd_cents DESC)")
}
