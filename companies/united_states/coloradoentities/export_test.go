package coloradoentities

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestExportWritesParquetFilesAndManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "snap.ndjson")
	copyFixtureSnapshot(t, snapshotPath)

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
	if result.RecordsSeen != 4 || result.RecordsExported != 3 || result.DecodeErrors != 1 {
		t.Fatalf("result counts = seen %d exported %d decode %d, want 4/3/1", result.RecordsSeen, result.RecordsExported, result.DecodeErrors)
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

	manifestDir := filepath.Dir(result.ManifestPath)
	wantRows := map[string]int64{
		"companies":         3,
		"company_names":     4, // 3 legal + 1 source_variant (delinquent record)
		"legal_forms":       3,
		"addresses":         7,
		"registered_agents": 3,
		"identifiers":       3,
		"source_evidence":   3,
	}
	for name, rows := range wantRows {
		exportFile := exportFileByName(manifest.Files, name)
		if exportFile == nil {
			t.Fatalf("missing export file %s", name)
		}
		if exportFile.RowCount != rows {
			t.Fatalf("%s row count = %d, want %d", name, exportFile.RowCount, rows)
		}
		if exportFile.SHA256 == "" || exportFile.SchemaHash == "" {
			t.Fatalf("%s missing hashes: %#v", name, exportFile)
		}
		exportPath := filepath.Join(manifestDir, exportFile.Path)
		hash, _, err := countryimport.HashFileSHA256(exportPath)
		if err != nil {
			t.Fatalf("hash export file %s: %v", exportPath, err)
		}
		if exportFile.SHA256 != hash {
			t.Fatalf("%s manifest SHA256 mismatch", name)
		}
		assertParquetRowCount(t, exportPath, rows)
	}

	companyRows := readParquetRows[CompanyExportRow](t, filepath.Join(manifestDir, exportFileByName(manifest.Files, "companies").Path))
	if len(companyRows) != 3 || companyRows[0].GlobalCompanyID != "CO:20251665680" {
		t.Fatalf("company rows = %#v", companyRows)
	}
}

func TestExportHonorsLimit(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "snap.ndjson")
	copyFixtureSnapshot(t, snapshotPath)

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

func TestExportUsesLatestSnapshotWhenPathBlank(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "colorado_entities_20260101T120000.000000000Z.ndjson")
	copyFixtureSnapshot(t, snapshotPath)

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{DataDir: dataDir, RunID: "auto"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.RecordsExported != 3 {
		t.Fatalf("RecordsExported = %d, want 3", result.RecordsExported)
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
