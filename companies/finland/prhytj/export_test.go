package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestSourceExportWritesParquetFilesAndManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: " " + snapshotPath + " ",
		RunID:        " run-1 ",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.RunID != "run-1" {
		t.Fatalf("result run ID = %q, want run-1", result.RunID)
	}
	if result.ManifestPath == "" {
		t.Fatal("manifest path is empty")
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.ExportKind != "source" || manifest.RunID != "run-1" || manifest.RecordsExported != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	if manifest.SourceSlug == nil || *manifest.SourceSlug != "prhytj" {
		t.Fatalf("manifest source slug = %v, want prhytj", manifest.SourceSlug)
	}
	if len(manifest.Inputs) != 1 {
		t.Fatalf("manifest inputs len = %d, want 1", len(manifest.Inputs))
	}
	if manifest.Inputs[0].Path != snapshotPath {
		t.Fatalf("manifest input path = %q, want %q", manifest.Inputs[0].Path, snapshotPath)
	}
	snapshotSHA, _, err := countryimport.HashFileSHA256(snapshotPath)
	if err != nil {
		t.Fatalf("hash snapshot: %v", err)
	}
	if manifest.Inputs[0].SHA256 != snapshotSHA {
		t.Fatalf("manifest input sha = %q, want %q", manifest.Inputs[0].SHA256, snapshotSHA)
	}

	manifestDir := filepath.Dir(result.ManifestPath)
	for _, name := range []string{"raw_records", "companies", "company_names", "legal_forms", "industries", "addresses", "registered_entries", "tax_registrations", "websites"} {
		exportFile := exportFileByName(manifest.Files, name)
		if exportFile == nil {
			t.Fatalf("missing export file %s", name)
		}
		if exportFile.RowCount < 0 {
			t.Fatalf("%s row count = %d, want non-negative", name, exportFile.RowCount)
		}
		if exportFile.SHA256 == "" {
			t.Fatalf("%s SHA256 is empty", name)
		}
		if exportFile.SchemaHash == "" {
			t.Fatalf("%s schema hash is empty", name)
		}
		exportPath := filepath.Join(manifestDir, exportFile.Path)
		relPath, err := filepath.Rel(manifestDir, exportPath)
		if err != nil {
			t.Fatalf("rel export path %s: %v", name, err)
		}
		if relPath == ".." || len(relPath) >= 3 && relPath[:3] == "../" || filepath.IsAbs(exportFile.Path) {
			t.Fatalf("%s path escapes manifest directory: %q", name, exportFile.Path)
		}
		if _, err := os.Stat(exportPath); err != nil {
			t.Fatalf("stat export file %s: %v", exportPath, err)
		}
	}
}

func TestSourceExportContinuesAfterInvalidLine(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(
		validSnapshotLine("0100130-4")+"\n"+
			`{"businessId":`+"\n"+
			validSnapshotLine("0112038-9")+"\n",
	))

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: snapshotPath,
		RunID:        "run-invalid-line",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.RecordsSeen != 3 || result.RecordsExported != 2 || result.DecodeErrors != 1 {
		t.Fatalf("result counts = seen %d exported %d decode %d, want 3/2/1", result.RecordsSeen, result.RecordsExported, result.DecodeErrors)
	}

	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.RecordsSeen != 3 || manifest.RecordsExported != 2 || manifest.DecodeErrors != 1 {
		t.Fatalf("manifest counts = seen %d exported %d decode %d, want 3/2/1", manifest.RecordsSeen, manifest.RecordsExported, manifest.DecodeErrors)
	}
}

func TestSourceExportLimitStopsAfterRequestedValidRows(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(validSnapshotLine("0100130-4")+"\n"+validSnapshotLine("0112038-9")+"\n"))

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
		t.Fatalf("records exported = %d, want 1", result.RecordsExported)
	}
}

func TestSourceExportUsesLatestSnapshotWhenPathBlank(t *testing.T) {
	dataDir := t.TempDir()
	olderPath := filepath.Join(dataDir, "snapshots", "older.ndjson")
	newerPath := filepath.Join(dataDir, "snapshots", "newer.ndjson")
	writeTestFile(t, olderPath, []byte(validSnapshotLine("0100130-4")+"\n"))
	writeTestFile(t, newerPath, []byte(validSnapshotLine("0112038-9")+"\n"))

	olderTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newerTime := olderTime.Add(time.Hour)
	if err := os.Chtimes(olderPath, olderTime, olderTime); err != nil {
		t.Fatalf("set older snapshot time: %v", err)
	}
	if err := os.Chtimes(newerPath, newerTime, newerTime); err != nil {
		t.Fatalf("set newer snapshot time: %v", err)
	}

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir: dataDir,
		RunID:   "run-latest",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if len(manifest.Inputs) != 1 || manifest.Inputs[0].Path != newerPath {
		t.Fatalf("manifest inputs = %#v, want newer path %q", manifest.Inputs, newerPath)
	}
}

type schemaHashSameA struct {
	Value string `parquet:"value"`
}

type schemaHashSameB struct {
	Value string `parquet:"value"`
}

type schemaHashDifferentTag struct {
	Value string `parquet:"renamed_value"`
}

type schemaHashDifferentType struct {
	Value int64 `parquet:"value"`
}

func TestSchemaHashForRowsUsesFieldSchema(t *testing.T) {
	hashA := schemaHashForRows[schemaHashSameA]()
	hashB := schemaHashForRows[schemaHashSameB]()
	if hashA != hashB {
		t.Fatalf("same field schema hashes differ: %s != %s", hashA, hashB)
	}
	if hashA == schemaHashForRows[schemaHashDifferentTag]() {
		t.Fatalf("schema hash did not change when parquet tag changed")
	}
	if hashA == schemaHashForRows[schemaHashDifferentType]() {
		t.Fatalf("schema hash did not change when field type changed")
	}

	legacyPayload := []byte("prhytj.schemaHashSameA")
	legacyHash := sha256.Sum256(legacyPayload)
	if hashA == hex.EncodeToString(legacyHash[:]) {
		t.Fatalf("schema hash still matches legacy type-name hash")
	}
}

func writeTestFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}

func validSnapshotLine(businessID string) string {
	return `{"businessId":{"value":"` + businessID + `"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`
}

func exportFileByName(files []countryimport.ExportFile, name string) *countryimport.ExportFile {
	for i := range files {
		if files[i].Name == name {
			return &files[i]
		}
	}
	return nil
}
