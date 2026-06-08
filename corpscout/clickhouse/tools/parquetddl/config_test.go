package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig([]byte(`
database: corpscout_sources
source_prefix: fi_prhytj
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
	require.Equal(t, "corpscout_sources", cfg.Database)
	require.Equal(t, "fi_prhytj", cfg.SourcePrefix)
	require.Equal(t, "companies.parquet", cfg.Tables["companies"].Parquet)
	require.Equal(t, []string{"business_id", "source_run_id"}, cfg.Tables["companies"].OrderBy)
	require.Equal(t, "UUID", cfg.Tables["companies"].InjectColumns["source_export_id"])
}
