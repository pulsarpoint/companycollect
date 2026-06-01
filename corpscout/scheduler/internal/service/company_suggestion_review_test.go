package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/service"
)

func suggestionRows(id uuid.UUID, targetCompanyID, createdCompanyID pgtype.UUID) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "target_company_id", "created_company_id", "source_id", "source_type",
		"source_input_table", "source_input_id", "source_native_id", "source_payload_hash",
		"source_pull_run_id", "confidence", "status", "reviewed_by", "reviewed_at",
		"review_note", "created_at", "updated_at",
	}).AddRow(
		id, targetCompanyID, createdCompanyID, uuid.New(), "brreg",
		"brreg_workflow.raw_records", uuid.NewString(), "123456789", "hash",
		pgtype.UUID{}, (*float32)(nil), "pending", (*string)(nil), pgtype.Timestamptz{},
		(*string)(nil), time.Time{}, time.Time{},
	)
}

func suggestionProfileRows(profileID, suggestionID, countryID uuid.UUID, name string) *pgxmock.Rows {
	country := pgUUID(countryID)
	registrationNumber := "123456789"
	lifecycleStatus := "active"
	website := "https://example.test"

	return pgxmock.NewRows([]string{
		"id", "suggestion_id", "target_row_id", "operation", "status", "confidence",
		"reviewed_by", "reviewed_at", "review_note", "name", "display_name",
		"canonical_slug", "country_id", "registration_number", "lei", "lifecycle_status",
		"website", "short_name", "short_description", "description", "founded_year",
		"employee_count", "revenue_usd", "parent_lei", "ultimate_parent_lei",
		"employee_estimate", "revenue_estimate", "ownership", "evidence",
		"created_at", "updated_at",
	}).AddRow(
		profileID, suggestionID, pgtype.UUID{}, "add", "pending", (*float32)(nil),
		(*string)(nil), pgtype.Timestamptz{}, (*string)(nil), &name, (*string)(nil),
		(*string)(nil), country, &registrationNumber, (*string)(nil), &lifecycleStatus,
		&website, (*string)(nil), (*string)(nil), (*string)(nil), (*int32)(nil),
		(*int32)(nil), (*int64)(nil), (*string)(nil), (*string)(nil),
		[]byte("{}"), []byte("{}"), []byte("{}"), []byte("{}"),
		time.Time{}, time.Time{},
	)
}

func TestApplyCompanySuggestionSections_ProfileCreatesCompanyAndPartiallyApplies(t *testing.T) {
	ctx := context.Background()
	suggestionID := uuid.New()
	profileID := uuid.New()
	countryID := uuid.New()
	companyID := uuid.New()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM suggestions`).WithArgs(suggestionID).
		WillReturnRows(suggestionRows(suggestionID, pgtype.UUID{}, pgtype.UUID{}))
	mock.ExpectQuery(`SELECT .* FROM suggestion_company_profiles`).WithArgs(profileID).
		WillReturnRows(suggestionProfileRows(profileID, suggestionID, countryID, "Example AS"))
	mock.ExpectQuery(`SELECT`).WithArgs("example-as").WillReturnError(pgx.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO companies`).WithArgs(
		pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
	).WillReturnRows(companyRows(companyID, "example-as", "Example AS", countryID))
	mock.ExpectExec(`UPDATE suggestions`).WithArgs(suggestionID, pgUUID(companyID)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`UPDATE suggestion_company_profiles`).WithArgs(profileID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectQuery(`SELECT .* AS pending_count`).WithArgs(suggestionID).
		WillReturnRows(pgxmock.NewRows([]string{"pending_count", "applied_count", "rejected_count"}).AddRow(int64(1), int64(1), int64(0)))
	mock.ExpectExec(`UPDATE suggestions`).WithArgs(suggestionID, "partially_applied", pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	err = service.ApplyCompanySuggestionSections(ctx, mock, suggestionID, []service.CompanySuggestionReviewItem{
		{Table: "suggestion_company_profiles", ID: profileID},
	}, "ops", "")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
