package db

import (
	"os"
	"strings"
	"testing"
)

func TestRetireLegacySourceRiverJobsMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000058_retire_legacy_source_river_jobs.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)
	for _, needle := range []string{
		"UPDATE river_job",
		"kind IN ('source_pull', 'source_process')",
		"state = 'cancelled'",
		"state NOT IN ('cancelled', 'completed', 'discarded')",
		"cleanup_reason",
		"legacy source_pull/source_process workers removed",
	} {
		if !strings.Contains(sql, needle) {
			t.Fatalf("retire migration missing %q", needle)
		}
	}
}
