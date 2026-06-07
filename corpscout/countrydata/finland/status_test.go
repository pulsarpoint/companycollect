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

func TestSourceStatusFromIncompleteOnlyExportReturnsMissing(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-1")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("create run dir: %v", err)
	}

	status, err := SourceStatusFromLatestManifest(dir, SourcePRHYTJ)
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

func TestSourceStatusFromLatestManifestSkipsIncompleteNewerRun(t *testing.T) {
	dir := t.TempDir()
	sourceSlug := SourcePRHYTJ
	manifestPath := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-1", "manifest.json")
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
	incompleteRunDir := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-2")
	if err := os.MkdirAll(incompleteRunDir, 0o755); err != nil {
		t.Fatalf("create incomplete run dir: %v", err)
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

func TestSourceStatusFromLatestManifestUsesLexicographicallyNewestCompleteRun(t *testing.T) {
	dir := t.TempDir()
	sourceSlug := SourcePRHYTJ
	olderManifestPath := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-1", "manifest.json")
	if err := countryimport.SaveExportManifest(olderManifestPath, countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     CountryISO2,
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           "run-1",
		SchemaVersion:   "finland.prhytj.source.v1",
		CreatedAt:       time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC),
		Files:           []countryimport.ExportFile{{Name: "companies", Path: "companies.parquet", RowCount: 1}},
		RecordsSeen:     1,
		RecordsExported: 1,
	}); err != nil {
		t.Fatalf("save older manifest: %v", err)
	}
	newerManifestPath := filepath.Join(dir, "sources", SourcePRHYTJ, "exports", "run-3", "manifest.json")
	if err := countryimport.SaveExportManifest(newerManifestPath, countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     CountryISO2,
		SourceSlug:      &sourceSlug,
		ExportKind:      "source",
		RunID:           "run-3",
		SchemaVersion:   "finland.prhytj.source.v1",
		CreatedAt:       time.Date(2026, 6, 7, 13, 0, 0, 0, time.UTC),
		Files:           []countryimport.ExportFile{{Name: "companies", Path: "companies.parquet", RowCount: 3}},
		RecordsSeen:     3,
		RecordsExported: 3,
	}); err != nil {
		t.Fatalf("save newer manifest: %v", err)
	}

	status, err := SourceStatusFromLatestManifest(dir, SourcePRHYTJ)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "exported" || status.LastExportManifestPath != newerManifestPath {
		t.Fatalf("status = %#v", status)
	}
	if status.RecordsExported != 3 {
		t.Fatalf("records exported = %d, want 3", status.RecordsExported)
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
