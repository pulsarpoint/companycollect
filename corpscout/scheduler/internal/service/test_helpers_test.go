package service_test

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	pgxmock "github.com/pashagolub/pgxmock/v3"
)

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

func companyRows(companyID uuid.UUID, slug, name string, countryID uuid.UUID) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "lei", "name", "country_id", "registration_number", "status", "primary_source_id",
		"created_at", "updated_at", "short_name", "short_description", "description", "website",
		"founded_year", "employee_estimate", "revenue_estimate", "ownership", "parent_lei",
		"ultimate_parent_lei", "canonical_slug", "display_name", "resolution_status", "evidence",
		"employee_count", "revenue_usd",
	}).AddRow(
		companyID, (*string)(nil), name, countryID, (*string)(nil), "active", pgtype.UUID{},
		time.Time{}, time.Time{}, (*string)(nil), (*string)(nil), (*string)(nil), (*string)(nil),
		(*int32)(nil), []byte(nil), []byte(nil), []byte(nil), (*string)(nil),
		(*string)(nil), slug, (*string)(nil), "resolved", []byte(nil),
		(*int32)(nil), (*int64)(nil),
	)
}
