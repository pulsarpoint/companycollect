package db

import (
	"os"
	"strings"
	"testing"
)

func TestCleanupSourceAuthMetadataMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000062_cleanup_source_auth_metadata.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	for _, needle := range []string{
		"name = 'opencorporates'",
		"config->>'auth_env' = 'CRAWLER_OPENCORPORATES_API_KEY'",
		"jsonb_set(config, '{auth_env}', '\"OPENCORPORATES_API_KEY\"'::jsonb)",
	} {
		if !strings.Contains(sql, needle) {
			t.Fatalf("source auth metadata migration missing %q", needle)
		}
	}
}
