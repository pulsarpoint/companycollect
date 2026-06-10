package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestSourceRegistryPruneMigrationKeepsOnlyCanonicalCompanySources(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000112_prune_legacy_data_sources.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"UPDATE companies",
		"UPDATE company_aliases",
		"UPDATE legacy_brreg_company_raw_inputs",
		"SET source_pull_run_id = NULL",
		"DELETE FROM data_sources",
		"'finland/prhytj'",
		"'united_states/coloradoentities'",
		"'united_states/irseobmf'",
		"'united_states/secedgar'",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}

	legacySources := []string{
		"global/ariregister",
		"norway/brreg",
		"global/cvr",
		"global/france",
		"united states/united_states_sec_edgar",
	}
	for _, needle := range legacySources {
		if strings.Contains(sql, needle) {
			t.Fatalf("migration should not keep legacy source %q", needle)
		}
	}
}
