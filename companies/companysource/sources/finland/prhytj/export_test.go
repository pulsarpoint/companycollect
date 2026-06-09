package prhytj

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
	"github.com/stretchr/testify/require"
)

func TestExportParquetWritesFlatRunFolder(t *testing.T) {
	runDir := t.TempDir()
	sourceFile := filepath.Join(runDir, "source.ndjson")
	payload, err := os.ReadFile(filepath.Join("testdata", "prh_snapshot_mixed.ndjson"))
	require.NoError(t, err)
	firstLine := strings.SplitN(string(payload), "\n", 2)[0] + "\n"
	require.NoError(t, os.WriteFile(sourceFile, []byte(firstLine), 0o644))

	result, err := NewSource(Config{}).ExportParquet(t.Context(), sourcespec.ExportParquetOptions{
		RunDir: runDir,
	})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(runDir, "manifest.json"), result.ManifestPath)
	require.EqualValues(t, 1, result.RecordsSeen)
	require.EqualValues(t, 1, result.RecordsExported)

	for _, name := range []string{
		"raw_records.parquet",
		"companies.parquet",
		"company_names.parquet",
		"legal_forms.parquet",
		"industries.parquet",
		"addresses.parquet",
		"registered_entries.parquet",
		"tax_registrations.parquet",
		"websites.parquet",
		"manifest.json",
	} {
		_, err := os.Stat(filepath.Join(runDir, name))
		require.NoErrorf(t, err, "%s should exist", name)
	}

	nested, err := filepath.Glob(filepath.Join(runDir, "*", "*"))
	require.NoError(t, err)
	require.Empty(t, nested)
}
