package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDownload(t *testing.T) {
	cfg, err := parseArgs([]string{"download", "--country", "finland", "--source", "prhytj", "--run-dir", "/runs/fi"})
	require.NoError(t, err)
	require.Equal(t, "download", cfg.Command)
	require.Equal(t, "finland", cfg.Country)
	require.Equal(t, "prhytj", cfg.Source)
	require.Equal(t, "/runs/fi", cfg.RunDir)
}

func TestParseExportParquet(t *testing.T) {
	cfg, err := parseArgs([]string{"export-parquet", "--country", "finland", "--source", "prhytj", "--run-dir", "/runs/fi"})
	require.NoError(t, err)
	require.Equal(t, "export-parquet", cfg.Command)
}

func TestParseRejectsClickHouseCommands(t *testing.T) {
	_, err := parseArgs([]string{"generate-clickhouse-migration", "--country", "finland", "--source", "prhytj"})
	require.EqualError(t, err, "unknown command \"generate-clickhouse-migration\"")

	_, err = parseArgs([]string{"import-clickhouse", "--country", "finland", "--source", "prhytj"})
	require.EqualError(t, err, "unknown command \"import-clickhouse\"")
}
