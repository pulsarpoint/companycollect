package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeDescriber map[string][]Column

func (d fakeDescriber) Describe(path string) ([]Column, error) {
	return d[path], nil
}

func TestGenerateMigrations(t *testing.T) {
	cfg, err := ParseConfig([]byte(`
database: corpscout_sources
tables:
  companies:
    parquet: companies.parquet
    table: fi_prhytj_companies
    engine: ReplacingMergeTree
    order_by: [business_id, source_run_id]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
`))
	require.NoError(t, err)

	up, down, err := GenerateMigrations(cfg, "/exports", fakeDescriber{
		"/exports/companies.parquet": {
			{Name: "business_id", Type: "String"},
			{Name: "source_run_id", Type: "String"},
		},
	})
	require.NoError(t, err)

	require.Contains(t, up, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_companies`")
	require.Contains(t, up, "`business_id` String")
	require.Contains(t, up, "`ingested_at` DateTime64(3, 'UTC')")
	require.Contains(t, up, "ORDER BY (`business_id`, `source_run_id`)")
	require.Contains(t, down, "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_companies`;")
}

func TestGenerateMigrationsRejectsInjectedColumnDuplicate(t *testing.T) {
	cfg, err := ParseConfig([]byte(`
database: corpscout_sources
tables:
  companies:
    parquet: companies.parquet
    table: fi_prhytj_companies
    engine: ReplacingMergeTree
    order_by: [business_id]
    inject_columns:
      source_export_id: UUID
`))
	require.NoError(t, err)

	_, _, err = GenerateMigrations(cfg, "/exports", fakeDescriber{
		"/exports/companies.parquet": {{Name: "source_export_id", Type: "UUID"}},
	})
	require.EqualError(t, err, "table companies injected column source_export_id duplicates parquet column")
}
