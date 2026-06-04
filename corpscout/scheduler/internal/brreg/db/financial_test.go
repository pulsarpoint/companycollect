package brregdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestClaimBrregCompanyFinancialBatchOnlyClaimsCompaniesWithoutStatements(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	neutralizeExistingFinancialClaims(t, tx)

	withStatements := seedFinancialClaimCompany(t, tx, "999777101", "WITH FINANCIALS AS")
	withoutStatements := seedFinancialClaimCompany(t, tx, "999777102", "WITHOUT FINANCIALS AS")
	seedFinancialStatement(t, tx, withStatements.CompanyID, withStatements.RawRecordID, 2024)

	workerID := "financial-claim-test"
	rows, err := db.New(tx).ClaimBrregCompanyFinancialBatch(ctx, db.ClaimBrregCompanyFinancialBatchParams{
		MaxParallelTasks: 10,
		Limit:            10,
		MaxAttempts:      3,
		WorkerID:         &workerID,
		LeaseSeconds:     900,
	})

	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, withoutStatements.CompanyID, rows[0].CompanyID)
	require.Equal(t, "running", rows[0].FinancialStatus)
	require.EqualValues(t, 1, rows[0].FinancialAttemptCount)
	require.NotNil(t, rows[0].FinancialLeaseBy)
	require.Equal(t, workerID, *rows[0].FinancialLeaseBy)
}

func TestClaimBrregCompanyFinancialBatchReclaimsStaleLeaseButLeavesActiveLease(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	neutralizeExistingFinancialClaims(t, tx)

	stale := seedFinancialClaimCompany(t, tx, "999777103", "STALE FINANCIAL AS")
	active := seedFinancialClaimCompany(t, tx, "999777104", "ACTIVE FINANCIAL AS")
	now := time.Now().UTC()
	_, err := tx.Exec(ctx, `
UPDATE brreg_source.company_process_status
SET
  financial_status = 'running',
  financial_attempt_count = 1,
  financial_lease_by = 'old-worker',
  financial_lease_until = $2
WHERE company_id = $1
`, stale.CompanyID, now.Add(-time.Minute))
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
UPDATE brreg_source.company_process_status
SET
  financial_status = 'running',
  financial_attempt_count = 1,
  financial_lease_by = 'active-worker',
  financial_lease_until = $2
WHERE company_id = $1
`, active.CompanyID, now.Add(time.Hour))
	require.NoError(t, err)

	workerID := "financial-reclaim-test"
	rows, err := db.New(tx).ClaimBrregCompanyFinancialBatch(ctx, db.ClaimBrregCompanyFinancialBatchParams{
		MaxParallelTasks: 10,
		Limit:            10,
		MaxAttempts:      3,
		WorkerID:         &workerID,
		LeaseSeconds:     900,
	})

	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, stale.CompanyID, rows[0].CompanyID)
	require.EqualValues(t, 2, rows[0].FinancialAttemptCount)
	require.NotNil(t, rows[0].FinancialLeaseBy)
	require.Equal(t, workerID, *rows[0].FinancialLeaseBy)
}

func neutralizeExistingFinancialClaims(t *testing.T, tx pgx.Tx) {
	t.Helper()
	_, err := tx.Exec(context.Background(), `
UPDATE brreg_source.company_process_status
SET
  financial_status = 'succeeded',
  financial_lease_by = NULL,
  financial_lease_until = NULL
`)
	require.NoError(t, err)
}

type seededFinancialClaimCompany struct {
	CompanyID   uuid.UUID
	RawRecordID uuid.UUID
}

func seedFinancialClaimCompany(t *testing.T, tx pgx.Tx, organizationNumber string, organizationName string) seededFinancialClaimCompany {
	t.Helper()
	ctx := context.Background()
	rawRecordID := uuid.New()
	companyID := uuid.New()
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
	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id,
  raw_record_id,
  organization_number,
  source_native_id,
  organization_name,
  organization_name_normalized,
  country_iso2,
  lifecycle_status,
  registration_status,
  row_status,
  payload_hash
) VALUES ($1, $2, $3, $3, $4, lower($4), 'NO', 'active', 'active', 'active', $5)
`, companyID, rawRecordID, organizationNumber, organizationName, payloadHash)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.company_process_status (company_id)
VALUES ($1)
ON CONFLICT (company_id) DO NOTHING
`, companyID)
	require.NoError(t, err)
	return seededFinancialClaimCompany{CompanyID: companyID, RawRecordID: rawRecordID}
}

func seedFinancialStatement(t *testing.T, tx pgx.Tx, companyID uuid.UUID, rawRecordID uuid.UUID, fiscalYear int) {
	t.Helper()
	_, err := tx.Exec(context.Background(), `
INSERT INTO brreg_source.financial_statements (
  company_id,
  raw_record_id,
  fiscal_year,
  statement_type,
  is_consolidated,
  original_currency,
  revenue_original_amount,
  source_url,
  evidence,
  raw_financial_payload
) VALUES ($1, $2, $3, 'annual_accounts', false, 'NOK', 1000.00, 'https://data.brreg.no/test', '{}'::jsonb, '{}'::jsonb)
`, companyID, rawRecordID, fiscalYear)
	require.NoError(t, err)
}
