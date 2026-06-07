package secedgar

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestExportWritesParquetFilesAndManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "company_tickers.json")
	writeSECTestFile(t, snapshotPath, []byte(`{
		"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
		"1":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."}
	}`))

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: " " + snapshotPath + " ",
		RunID:        " run-1 ",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.SourceSlug != SourceSlug || result.RunID != "run-1" {
		t.Fatalf("result = %#v", result)
	}
	if result.RecordsSeen != 2 || result.RecordsExported != 2 || result.DecodeErrors != 0 {
		t.Fatalf("result counts = seen %d exported %d decode %d, want 2/2/0", result.RecordsSeen, result.RecordsExported, result.DecodeErrors)
	}

	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.CountryISO2 != "US" || manifest.ExportKind != "source" || manifest.RunID != "run-1" {
		t.Fatalf("manifest = %#v", manifest)
	}
	if manifest.SchemaVersion != SourceExportSchemaVersion {
		t.Fatalf("manifest SchemaVersion = %q, want %q", manifest.SchemaVersion, SourceExportSchemaVersion)
	}
	if manifest.SourceSlug == nil || *manifest.SourceSlug != SourceKey {
		t.Fatalf("manifest source slug = %v, want %q", manifest.SourceSlug, SourceKey)
	}
	if len(manifest.Inputs) != 1 || manifest.Inputs[0].Path != snapshotPath {
		t.Fatalf("manifest inputs = %#v, want snapshot %q", manifest.Inputs, snapshotPath)
	}
	snapshotSHA, _, err := countryimport.HashFileSHA256(snapshotPath)
	if err != nil {
		t.Fatalf("hash snapshot: %v", err)
	}
	if manifest.Inputs[0].SHA256 != snapshotSHA {
		t.Fatalf("manifest input sha = %q, want %q", manifest.Inputs[0].SHA256, snapshotSHA)
	}
	if manifest.RecordsSeen != 2 || manifest.RecordsExported != 2 || manifest.DecodeErrors != 0 {
		t.Fatalf("manifest counts = seen %d exported %d decode %d, want 2/2/0", manifest.RecordsSeen, manifest.RecordsExported, manifest.DecodeErrors)
	}

	manifestDir := filepath.Dir(result.ManifestPath)
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
		if exportFile.SHA256 == "" || exportFile.SchemaHash == "" {
			t.Fatalf("%s missing hashes: %#v", tt.name, exportFile)
		}
		if filepath.IsAbs(exportFile.Path) {
			t.Fatalf("%s path is absolute: %q", tt.name, exportFile.Path)
		}
		exportPath := filepath.Join(manifestDir, exportFile.Path)
		hash, _, err := countryimport.HashFileSHA256(exportPath)
		if err != nil {
			t.Fatalf("hash export file %s: %v", exportPath, err)
		}
		if exportFile.SHA256 != hash {
			t.Fatalf("%s manifest SHA256 = %q, want %q", tt.name, exportFile.SHA256, hash)
		}
		assertParquetRowCount(t, exportPath, tt.rows)
	}
}

func TestExportHonorsLimit(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "company_tickers.json")
	writeSECTestFile(t, snapshotPath, []byte(`{
		"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
		"1":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."}
	}`))

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: snapshotPath,
		RunID:        "run-limit",
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.RecordsSeen != 2 || result.RecordsExported != 1 {
		t.Fatalf("result counts = seen %d exported %d, want 2/1", result.RecordsSeen, result.RecordsExported)
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

func exportFileByName(files []countryimport.ExportFile, name string) *countryimport.ExportFile {
	for i := range files {
		if files[i].Name == name {
			return &files[i]
		}
	}
	return nil
}
