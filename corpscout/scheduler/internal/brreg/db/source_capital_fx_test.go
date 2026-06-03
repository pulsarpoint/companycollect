package brregdb

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestConvertSourceCapitalToUSDUsesLocalExchangeRates(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	companyID := insertSourceCompanyForCapitalFX(t, tx, "981276957", "BORTIGARD FX TEST AS")
	capitalID := uuid.New()
	_, err := tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id,
  company_id,
  raw_record_id,
  capital_type,
  original_amount,
  original_currency,
  raw_capital_payload
) VALUES (
  $1,
  $2,
  (SELECT raw_record_id FROM brreg_source.companies WHERE id = $2),
  'Aksjekapital',
  11500.00,
  'NOK',
  '{}'::jsonb
)`, capitalID, companyID)
	require.NoError(t, err)
	insertExchangeRateSheetForCapitalFX(t, tx, "2026-06-03", map[string]string{
		"EUR": "1.000000000000",
		"USD": "1.090000000000",
		"NOK": "11.500000000000",
	})

	result, err := New(tx).ConvertSourceCapitalToUSD(ctx, ConvertSourceCapitalToUSDCommand{
		IDs:     []string{companyID.String()},
		Limit:   10,
		Trigger: "manual",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.CapitalSeen)
	require.EqualValues(t, 1, result.CapitalConverted)
	require.EqualValues(t, 0, result.CapitalSkippedMissingRate)
	require.EqualValues(t, 0, result.CapitalSkippedAlreadyConverted)

	var amountUSDCents int64
	var fxSource string
	var fxRateDate time.Time
	var fxMetadata json.RawMessage
	err = tx.QueryRow(ctx, `
SELECT amount_usd_cents, fx_source, fx_rate_date, fx_metadata
FROM brreg_source.capital
WHERE id = $1
`, capitalID).Scan(&amountUSDCents, &fxSource, &fxRateDate, &fxMetadata)
	require.NoError(t, err)
	require.EqualValues(t, 109000, amountUSDCents)
	require.Equal(t, "ecb", fxSource)
	require.Equal(t, "2026-06-03", fxRateDate.Format("2006-01-02"))
	require.Contains(t, string(fxMetadata), `"source_currency"`)
	require.Contains(t, string(fxMetadata), `"source_rate_per_base"`)
}

func TestConvertSourceCapitalToUSDSkipsAlreadyConvertedUnlessForced(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	companyID := insertSourceCompanyForCapitalFX(t, tx, "992768403", "FORCED FX TEST AS")
	capitalID := uuid.New()
	_, err := tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id,
  company_id,
  raw_record_id,
  capital_type,
  original_amount,
  original_currency,
  amount_usd_cents,
  fx_source,
  fx_rate_date,
  raw_capital_payload
) VALUES (
  $1,
  $2,
  (SELECT raw_record_id FROM brreg_source.companies WHERE id = $2),
  'Aksjekapital',
  100.00,
  'USD',
  9900,
  'ecb',
  '2026-06-02',
  '{}'::jsonb
)`, capitalID, companyID)
	require.NoError(t, err)
	insertExchangeRateSheetForCapitalFX(t, tx, "2026-06-03", map[string]string{
		"EUR": "1.000000000000",
		"USD": "1.090000000000",
	})

	skipped, err := New(tx).ConvertSourceCapitalToUSD(ctx, ConvertSourceCapitalToUSDCommand{
		IDs:   []string{companyID.String()},
		Limit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, skipped.CapitalConverted)
	require.EqualValues(t, 1, skipped.CapitalSkippedAlreadyConverted)

	forced, err := New(tx).ConvertSourceCapitalToUSD(ctx, ConvertSourceCapitalToUSDCommand{
		IDs:            []string{companyID.String()},
		Limit:          10,
		ForceReprocess: true,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, forced.CapitalConverted)

	var amountUSDCents int64
	err = tx.QueryRow(ctx, `
SELECT amount_usd_cents
FROM brreg_source.capital
WHERE id = $1
`, capitalID).Scan(&amountUSDCents)
	require.NoError(t, err)
	require.EqualValues(t, 10000, amountUSDCents)
}

func TestConvertSourceCapitalToUSDSkipsRowsWithMissingRate(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	companyID := insertSourceCompanyForCapitalFX(t, tx, "912768403", "MISSING RATE FX TEST AS")
	capitalID := uuid.New()
	_, err := tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id,
  company_id,
  raw_record_id,
  capital_type,
  original_amount,
  original_currency,
  raw_capital_payload
) VALUES (
  $1,
  $2,
  (SELECT raw_record_id FROM brreg_source.companies WHERE id = $2),
  'Aksjekapital',
  100.00,
  'SEK',
  '{}'::jsonb
)`, capitalID, companyID)
	require.NoError(t, err)
	insertExchangeRateSheetForCapitalFX(t, tx, "2026-06-03", map[string]string{
		"EUR": "1.000000000000",
		"USD": "1.090000000000",
	})

	result, err := New(tx).ConvertSourceCapitalToUSD(ctx, ConvertSourceCapitalToUSDCommand{
		IDs:   []string{companyID.String()},
		Limit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.CapitalSeen)
	require.EqualValues(t, 0, result.CapitalConverted)
	require.EqualValues(t, 1, result.CapitalSkippedMissingRate)
	require.EqualValues(t, 0, result.CapitalSkippedAlreadyConverted)

	var amountUSDCents sql.NullInt64
	err = tx.QueryRow(ctx, `
SELECT amount_usd_cents
FROM brreg_source.capital
WHERE id = $1
`, capitalID).Scan(&amountUSDCents)
	require.NoError(t, err)
	require.False(t, amountUSDCents.Valid)
}

func insertSourceCompanyForCapitalFX(t *testing.T, tx pgx.Tx, organizationNumber string, organizationName string) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	rawRecordID := uuid.New()
	payloadHash := uuid.NewString()
	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id,
  source_native_id,
  organization_number,
  organization_name,
  registration_status,
  country_iso2,
  raw_payload,
  payload_hash
) VALUES ($1, $2, $2, $3, 'active', 'NO', '{}'::jsonb, $4)
`, rawRecordID, organizationNumber, organizationName, payloadHash)
	require.NoError(t, err)

	companyID := uuid.New()
	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id,
  raw_record_id,
  organization_number,
  organization_name,
  country_iso2,
  lifecycle_status,
  registration_status,
  row_status
) VALUES ($1, $2, $3, $4, 'NO', 'active', 'active', 'active')
`, companyID, rawRecordID, organizationNumber, organizationName)
	require.NoError(t, err)
	return companyID
}

func insertExchangeRateSheetForCapitalFX(t *testing.T, tx pgx.Tx, rateDate string, rates map[string]string) {
	t.Helper()
	ctx := t.Context()
	sourceFileID := uuid.New()
	sheetID := uuid.New()
	_, err := tx.Exec(ctx, `
INSERT INTO exchange_rate_source_files (
  id,
  provider,
  source_url,
  rate_date,
  content_sha256,
  content_length_bytes,
  status,
  processed_at
) VALUES ($1, 'ecb', 'https://example.test/ecb.xml', $2::date, $3, 1, 'processed', now())
`, sourceFileID, rateDate, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO exchange_rate_sheets (
  id,
  provider,
  rate_date,
  base_currency,
  source_file_id,
  content_sha256
) VALUES ($1, 'ecb', $2::date, 'EUR', $3, $4)
`, sheetID, rateDate, sourceFileID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)

	for currency, rate := range rates {
		_, err = tx.Exec(ctx, `
INSERT INTO exchange_rates (
  sheet_id,
  currency,
  rate_per_base
) VALUES ($1, $2, $3::numeric)
`, sheetID, currency, rate)
		require.NoError(t, err)
	}
}
