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
				Table:   "example_source_companies",
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
	require.Contains(t, up, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`example_source_companies`")
	require.Contains(t, up, "`source_export_id` UUID")
	require.Contains(t, up, "`ingested_at` DateTime64(3, 'UTC')")
	require.Contains(t, up, "ENGINE = ReplacingMergeTree")
	require.Contains(t, up, "ORDER BY (`business_id`, `source_run_id`)")
	require.Contains(t, up, "SETTINGS allow_nullable_key = 1;")
	require.Contains(t, down, "DROP TABLE IF EXISTS `corpscout_sources`.`example_source_companies`;")
	require.Equal(t, up, strings.TrimSuffix(up, "\n")+"\n")
}

func TestGenerateMigrationEscapesWeirdIdentifier(t *testing.T) {
	weirdColumn := "odd\\`name"
	cfg := Config{
		Database: "corpscout_sources",
		Tables: map[string]TableConfig{
			"companies": {
				Parquet: "companies.parquet",
				Table:   "example_source_companies",
				Engine:  "ReplacingMergeTree",
				OrderBy: []string{weirdColumn},
			},
		},
	}
	describer := fakeDescriber{
		"/exports/companies.parquet": {
			{Name: weirdColumn, Type: "String"},
		},
	}

	up, _, err := generateMigrations(cfg, "/exports", describer)
	require.NoError(t, err)
	expectedColumn := "`odd" + "\\\\" + "\\`" + "name`"
	require.Contains(t, up, expectedColumn+" String")
	require.Contains(t, up, "ORDER BY ("+expectedColumn+")")
}

func TestGenerateMigrationRejectsInjectedColumnDuplicate(t *testing.T) {
	cfg := Config{
		Database: "corpscout_sources",
		Tables: map[string]TableConfig{
			"companies": {
				Parquet: "companies.parquet",
				Table:   "example_source_companies",
				Engine:  "ReplacingMergeTree",
				OrderBy: []string{"business_id"},
				InjectColumns: map[string]string{
					"source_export_id": "UUID",
				},
			},
		},
	}
	describer := fakeDescriber{
		"/exports/companies.parquet": {
			{Name: "business_id", Type: "String"},
			{Name: "source_export_id", Type: "String"},
		},
	}

	_, _, err := generateMigrations(cfg, "/exports", describer)
	require.EqualError(t, err, "table companies injected column source_export_id duplicates parquet column")
}

func TestClickHouseStringLiteralEscapesPath(t *testing.T) {
	require.Equal(t, "'/exports/odd\\\\path\\'s.parquet'", clickHouseStringLiteral("/exports/odd\\path's.parquet"))
}
