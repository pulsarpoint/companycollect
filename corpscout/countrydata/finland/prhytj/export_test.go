package prhytj

import (
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestSourceExportWritesParquetFilesAndManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Export(t.Context(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: snapshotPath,
		RunID:        "run-1",
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if result.ManifestPath == "" {
		t.Fatal("manifest path is empty")
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.ExportKind != "source" || manifest.RecordsExported != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	if manifest.SourceSlug == nil || *manifest.SourceSlug != "prhytj" {
		t.Fatalf("manifest source slug = %v, want prhytj", manifest.SourceSlug)
	}
	for _, name := range []string{"companies", "company_names", "legal_forms", "industries", "addresses", "registered_entries", "tax_registrations", "websites"} {
		if exportFileByName(manifest.Files, name) == nil {
			t.Fatalf("missing export file %s", name)
		}
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

func exportFileByName(files []countryimport.ExportFile, name string) *countryimport.ExportFile {
	for i := range files {
		if files[i].Name == name {
			return &files[i]
		}
	}
	return nil
}
