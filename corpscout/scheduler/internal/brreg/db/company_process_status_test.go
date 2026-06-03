package brregdb

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestClaimCompaniesForTranslationEnsuresMissingStatusesBeforeSecondClaim(t *testing.T) {
	ctx := t.Context()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	workerID := "translation-worker-test"
	companyID := uuid.New()

	mock.ExpectQuery(`WITH active_capacity AS`).
		WithArgs(int32(5), int32(10), int32(3), &workerID, int32(900)).
		WillReturnRows(claimTranslationCompanyRows())
	mock.ExpectQuery(`WITH inserted AS`).
		WithArgs(int32(10)).
		WillReturnRows(pgxmock.NewRows([]string{"rows_inserted"}).AddRow(int32(1)))
	mock.ExpectQuery(`WITH active_capacity AS`).
		WithArgs(int32(5), int32(10), int32(3), &workerID, int32(900)).
		WillReturnRows(claimTranslationCompanyRows().AddRow(
			companyID,
			"running",
			int32(1),
			&workerID,
			pgtype.Timestamptz{Time: time.Now().Add(15 * time.Minute), Valid: true},
			pgtype.Timestamptz{Time: time.Now(), Valid: true},
			pgtype.Timestamptz{},
			nil,
			nil,
			nil,
			nil,
			[]byte(`{}`),
			"pending",
			int32(0),
			nil,
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			nil,
			nil,
			nil,
			nil,
			[]byte(`{}`),
			"pending",
			int32(0),
			nil,
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			nil,
			nil,
			nil,
			nil,
			[]byte(`{}`),
			time.Now(),
			time.Now(),
			"999111222",
			"CLAIM TEST AS",
		))

	result, err := New(mock).ClaimCompaniesForTranslation(ctx, ClaimCompaniesForTranslationCommand{
		Limit:            10,
		MaxParallelTasks: 5,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         &workerID,
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.StatusRowsInserted)
	require.Len(t, result.Companies, 1)
	require.Equal(t, companyID, result.Companies[0].CompanyID)
	require.Equal(t, "running", result.Companies[0].TranslationStatus)
	require.Equal(t, "999111222", result.Companies[0].OrganizationNumber)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClaimCompaniesForTranslationDoesNotEnsureWhenInitialClaimReturnsRows(t *testing.T) {
	ctx := t.Context()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	workerID := "translation-worker-test"
	companyID := uuid.New()

	mock.ExpectQuery(`WITH active_capacity AS`).
		WithArgs(int32(2), int32(1), int32(3), &workerID, int32(60)).
		WillReturnRows(claimTranslationCompanyRows().AddRow(
			companyID,
			"running",
			int32(2),
			&workerID,
			pgtype.Timestamptz{Time: time.Now().Add(time.Minute), Valid: true},
			pgtype.Timestamptz{Time: time.Now(), Valid: true},
			pgtype.Timestamptz{},
			nil,
			nil,
			nil,
			nil,
			[]byte(`{}`),
			"pending",
			int32(0),
			nil,
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			nil,
			nil,
			nil,
			nil,
			[]byte(`{}`),
			"pending",
			int32(0),
			nil,
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			pgtype.Timestamptz{},
			nil,
			nil,
			nil,
			nil,
			[]byte(`{}`),
			time.Now(),
			time.Now(),
			"999111333",
			"EXISTING CLAIM TEST AS",
		))

	result, err := New(mock).ClaimCompaniesForTranslation(ctx, ClaimCompaniesForTranslationCommand{
		Limit:            1,
		MaxParallelTasks: 2,
		LeaseSeconds:     60,
		MaxAttempts:      3,
		WorkerID:         &workerID,
	})

	require.NoError(t, err)
	require.Zero(t, result.StatusRowsInserted)
	require.Len(t, result.Companies, 1)
	require.Equal(t, companyID, result.Companies[0].CompanyID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func claimTranslationCompanyRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"company_id",
		"translation_status",
		"translation_attempt_count",
		"translation_lease_by",
		"translation_lease_until",
		"translation_last_started_at",
		"translation_last_finished_at",
		"translation_error",
		"translation_error_category",
		"translation_error_code",
		"translation_retry_strategy",
		"translation_metadata",
		"currency_status",
		"currency_attempt_count",
		"currency_lease_by",
		"currency_lease_until",
		"currency_last_started_at",
		"currency_last_finished_at",
		"currency_error",
		"currency_error_category",
		"currency_error_code",
		"currency_retry_strategy",
		"currency_metadata",
		"financial_status",
		"financial_attempt_count",
		"financial_lease_by",
		"financial_lease_until",
		"financial_last_started_at",
		"financial_last_finished_at",
		"financial_error",
		"financial_error_category",
		"financial_error_code",
		"financial_retry_strategy",
		"financial_metadata",
		"created_at",
		"updated_at",
		"organization_number",
		"organization_name",
	})
}
