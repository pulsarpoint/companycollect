package db_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestCompanyPayloadCleanupMigrationDropsRetiredStorage(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000110_remove_postgres_company_payload_storage.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"DROP SCHEMA IF EXISTS brreg_source CASCADE",
		"DROP SCHEMA IF EXISTS ariregister_source CASCADE",
		"DROP SCHEMA IF EXISTS france_source CASCADE",
		"DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE",
		"DROP TABLE IF EXISTS suggestion_company_profiles CASCADE",
		"DROP TABLE IF EXISTS company_financials CASCADE",
		"DROP TABLE IF EXISTS companies_house_company_raw_inputs CASCADE",
	}

	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}

	preserved := []string{
		"DROP TABLE IF EXISTS companies",
		"DROP TABLE IF EXISTS domains",
		"DROP TABLE IF EXISTS company_domains",
		"DROP TABLE IF EXISTS company_relationships",
		"DROP TABLE IF EXISTS data_sources",
	}
	for _, needle := range preserved {
		pattern := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(needle) + `(?:\s|;)`)
		if pattern.FindStringIndex(sql) != nil {
			t.Fatalf("cleanup migration should not drop preserved anchor %q", needle)
		}
	}
}
