package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestCompanySuggestionReviewModelMigrationDefinesParentAndSectionTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000046_company_suggestion_review_model.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"CREATE TABLE suggestions",
		"target_company_id UUID REFERENCES companies",
		"created_company_id UUID REFERENCES companies",
		"FOREIGN KEY (source_id, source_type) REFERENCES data_sources(id, name)",
		"CREATE TABLE suggestion_company_profiles",
		"CREATE TABLE suggestion_company_domains",
		"CREATE TABLE suggestion_company_locations",
		"CREATE TABLE suggestion_company_emails",
		"CREATE TABLE suggestion_company_phones",
		"CREATE TABLE suggestion_company_financials",
		"CREATE TABLE suggestion_company_industries",
		"CREATE TABLE suggestion_company_markets",
		"CREATE TABLE suggestion_company_services",
		"CREATE TABLE suggestion_company_relationships",
		"status IN ('pending', 'applied', 'rejected', 'superseded')",
	}

	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}
}
