package finland

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestLayoutForDataDir(t *testing.T) {
	layout := LayoutForDataDir("/data")
	if layout.SourceDir(SourcePRHYTJ) != filepath.Join("/data", "sources", SourcePRHYTJ) {
		t.Fatalf("source dir = %q", layout.SourceDir(SourcePRHYTJ))
	}
	if layout.FinalDir != filepath.Join("/data", "final") {
		t.Fatalf("final dir = %q", layout.FinalDir)
	}
}

func TestSourceStatusFromMissingManifest(t *testing.T) {
	status, err := SourceStatusFromLatestManifest(t.TempDir(), SourcePRHYTJ)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "missing" {
		t.Fatalf("status = %q, want missing", status.Status)
	}
}

func TestSourceStatusFromLatestManifest(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-1", "manifest.json")
	sourceSlug := SourcePRHYTJ
	if err := countryimport.SaveExportManifest(manifestPath, countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     CountryISO2,
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           "run-1",
		SchemaVersion:   "finland.prhytj.source.v1",
		CreatedAt:       time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC),
		Files:           []countryimport.ExportFile{{Name: "companies", Path: "companies.parquet", RowCount: 2}},
		RecordsSeen:     2,
		RecordsExported: 2,
	}); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	status, err := SourceStatusFromLatestManifest(dir, SourcePRHYTJ)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "exported" || status.LastExportManifestPath != manifestPath {
		t.Fatalf("status = %#v", status)
	}
	if status.RecordsExported != 2 {
		t.Fatalf("records exported = %d, want 2", status.RecordsExported)
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
