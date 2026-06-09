package secedgar

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

func TestExportParquetWritesRunFolderFilesAndManifest(t *testing.T) {
	runDir := t.TempDir()
	writeSECTestFile(t, filepath.Join(runDir, "source.json"), []byte(`{
		"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
		"1":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."}
	}`))

	source := NewSource(Config{})
	result, err := source.ExportParquet(t.Context(), sourcespec.ExportParquetOptions{RunDir: runDir})
	if err != nil {
		t.Fatalf("export parquet: %v", err)
	}
	if result.RecordsSeen != 2 || result.RecordsExported != 2 || result.DecodeErrors != 0 {
		t.Fatalf("counts = seen %d exported %d decode %d, want 2/2/0", result.RecordsSeen, result.RecordsExported, result.DecodeErrors)
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
	if len(manifest.Inputs) != 1 || manifest.Inputs[0].Path != "source.json" {
		t.Fatalf("manifest inputs = %#v, want source.json", manifest.Inputs)
	}

	for _, tt := range []struct {
		name string
		rows int64
	}{
		{name: "companies", rows: 2},
		{name: "company_names", rows: 2},
		{name: "identifiers", rows: 4},
		{name: "source_evidence", rows: 2},
	} {
		exportFile := exportFileByName(manifest.Files, tt.name)
		if exportFile == nil {
			t.Fatalf("missing export file %s", tt.name)
		}
		if exportFile.RowCount != tt.rows {
			t.Fatalf("%s row count = %d, want %d", tt.name, exportFile.RowCount, tt.rows)
		}
		assertParquetRowCount(t, filepath.Join(runDir, exportFile.Path), tt.rows)
	}

	companyRows := readParquetRows[CompanyExportRow](t, filepath.Join(runDir, exportFileByName(manifest.Files, "companies").Path))
	if len(companyRows) != 2 || companyRows[0].CIK10 != "0000320193" || companyRows[0].Ticker != "AAPL" {
		t.Fatalf("company rows = %#v", companyRows)
	}

	evidenceRows := readParquetRows[SourceEvidenceExportRow](t, filepath.Join(runDir, exportFileByName(manifest.Files, "source_evidence").Path))
	if len(evidenceRows) != 2 || !strings.Contains(evidenceRows[0].Evidence, `"ticker":"AAPL"`) {
		t.Fatalf("source evidence rows = %#v", evidenceRows)
	}
}

func TestExportParquetHonorsLimit(t *testing.T) {
	runDir := t.TempDir()
	writeSECTestFile(t, filepath.Join(runDir, "source.json"), []byte(`{
		"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
		"1":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."}
	}`))

	source := NewSource(Config{})
	result, err := source.ExportParquet(t.Context(), sourcespec.ExportParquetOptions{RunDir: runDir, Limit: 1})
	if err != nil {
		t.Fatalf("export parquet: %v", err)
	}
	if result.RecordsSeen != 2 || result.RecordsExported != 1 {
		t.Fatalf("counts = seen %d exported %d, want 2/1", result.RecordsSeen, result.RecordsExported)
	}
}

func writeSECTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir test file dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
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
