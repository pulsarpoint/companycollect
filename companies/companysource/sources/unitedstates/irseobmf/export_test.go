package irseobmf

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

func TestExportParquetWritesRunFolderFilesAndManifest(t *testing.T) {
	runDir := t.TempDir()
	copyFixtureSnapshot(t, filepath.Join(runDir, "source.ndjson"))

	source := NewSource(Config{})
	result, err := source.ExportParquet(t.Context(), sourcespec.ExportParquetOptions{RunDir: runDir})
	if err != nil {
		t.Fatalf("export parquet: %v", err)
	}
	if result.RunDir != runDir {
		t.Fatalf("RunDir = %q, want %q", result.RunDir, runDir)
	}
	if result.RecordsSeen != 4 || result.RecordsExported != 3 || result.DecodeErrors != 1 {
		t.Fatalf("counts = seen %d exported %d decode %d, want 4/3/1", result.RecordsSeen, result.RecordsExported, result.DecodeErrors)
	}

	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.CountryISO2 != "US" || manifest.ExportKind != "source" || manifest.RunID != filepath.Base(runDir) {
		t.Fatalf("manifest = %#v", manifest)
	}
	if manifest.SourceSlug == nil || *manifest.SourceSlug != SourceSlug {
		t.Fatalf("manifest source slug = %v, want %q", manifest.SourceSlug, SourceSlug)
	}
	if len(manifest.Inputs) != 1 || manifest.Inputs[0].Path != "source.ndjson" {
		t.Fatalf("manifest inputs = %#v, want source.ndjson", manifest.Inputs)
	}

	wantRows := map[string]int64{
		"companies":       3,
		"company_names":   4,
		"addresses":       3,
		"classifications": 3,
		"financials":      2,
		"identifiers":     4,
		"source_evidence": 3,
	}
	for name, rows := range wantRows {
		exportFile := exportFileByName(manifest.Files, name)
		if exportFile == nil {
			t.Fatalf("missing export file %s", name)
		}
		if exportFile.RowCount != rows {
			t.Fatalf("%s row count = %d, want %d", name, exportFile.RowCount, rows)
		}
		assertParquetRowCount(t, filepath.Join(runDir, exportFile.Path), rows)
	}

	companyRows := readParquetRows[CompanyExportRow](t, filepath.Join(runDir, exportFileByName(manifest.Files, "companies").Path))
	if len(companyRows) != 3 || companyRows[0].EIN != "010011694" || !companyRows[0].IsActiveExempt {
		t.Fatalf("company rows = %#v", companyRows)
	}
}

func TestExportParquetHonorsLimit(t *testing.T) {
	runDir := t.TempDir()
	copyFixtureSnapshot(t, filepath.Join(runDir, "source.ndjson"))

	source := NewSource(Config{})
	result, err := source.ExportParquet(t.Context(), sourcespec.ExportParquetOptions{RunDir: runDir, Limit: 1})
	if err != nil {
		t.Fatalf("export parquet: %v", err)
	}
	if result.RecordsExported != 1 {
		t.Fatalf("RecordsExported = %d, want 1", result.RecordsExported)
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	companies := exportFileByName(manifest.Files, "companies")
	if companies == nil || companies.RowCount != 1 {
		t.Fatalf("companies file = %#v, want 1 row", companies)
	}
}

func copyFixtureSnapshot(t *testing.T, dst string) {
	t.Helper()
	input, err := os.ReadFile(filepath.Join("testdata", "eo_bmf_sample.ndjson"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := os.WriteFile(dst, input, 0o644); err != nil {
		t.Fatalf("write fixture snapshot: %v", err)
	}
}

func assertParquetRowCount(t *testing.T, path string, want int64) {
	t.Helper()
	handle, err := os.Open(path)
	if err != nil {
		t.Fatalf("open parquet handle: %v", err)
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		t.Fatalf("stat parquet handle: %v", err)
	}
	file, err := parquet.OpenFile(handle, info.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	if file.NumRows() != want {
		t.Fatalf("%s NumRows = %d, want %d", path, file.NumRows(), want)
	}
}

func readParquetRows[T any](t *testing.T, path string) []T {
	t.Helper()
	handle, err := os.Open(path)
	if err != nil {
		t.Fatalf("open parquet handle: %v", err)
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		t.Fatalf("stat parquet handle: %v", err)
	}
	rows, err := parquet.Read[T](handle, info.Size())
	if err != nil {
		t.Fatalf("read parquet rows: %v", err)
	}
	return rows
}

func exportFileByName(files []countryimport.ExportFile, name string) *countryimport.ExportFile {
	for i := range files {
		if files[i].Name == name {
			return &files[i]
		}
	}
	return nil
}
