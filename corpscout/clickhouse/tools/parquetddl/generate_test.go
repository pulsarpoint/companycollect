package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeDescriber map[string][]Column

func (f fakeDescriber) Describe(path string) ([]Column, error) {
	return f[path], nil
}

func TestGenerateMigrationIsDeterministic(t *testing.T) {
	cfg := Config{
		Database: "corpscout_sources",
		Tables: map[string]TableConfig{
			"companies": {
				Parquet: "companies.parquet",
				Table:   "fi_prhytj_companies",
				Engine:  "ReplacingMergeTree",
				OrderBy: []string{"business_id", "source_run_id"},
				InjectColumns: map[string]string{
					"source_export_id": "UUID",
					"ingested_at":      "DateTime64(3, 'UTC')",
				},
			},
		},
	}
	describer := fakeDescriber{
		"/exports/companies.parquet": {
			{Name: "country_iso2", Type: "String"},
			{Name: "source_slug", Type: "String"},
			{Name: "business_id", Type: "String"},
			{Name: "source_run_id", Type: "String"},
		},
	}

	up, down, err := generateMigrations(cfg, "/exports", describer)
	require.NoError(t, err)
	require.Contains(t, up, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_companies`")
	require.Contains(t, up, "`source_export_id` UUID")
	require.Contains(t, up, "`ingested_at` DateTime64(3, 'UTC')")
	require.Contains(t, up, "ENGINE = ReplacingMergeTree")
	require.Contains(t, up, "ORDER BY (`business_id`, `source_run_id`)")
	require.Contains(t, down, "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_companies`;")
	require.Equal(t, up, strings.TrimSuffix(up, "\n")+"\n")
}
