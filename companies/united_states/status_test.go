package unitedstates

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestSourceStatusFromLatestManifestReturnsMissing(t *testing.T) {
	status, err := SourceStatusFromLatestManifest(t.TempDir(), SourceSECEdgar)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "missing" {
		t.Fatalf("Status = %q", status.Status)
	}
}

func TestSourceStatusFromLatestManifestSkipsIncompleteNewerRun(t *testing.T) {
	root := t.TempDir()
	older := filepath.Join(root, "sources", SourceSECEdgar, "exports", "20260607T010000Z-secedgar")
	newer := filepath.Join(root, "sources", SourceSECEdgar, "exports", "20260607T020000Z-secedgar")
	if err := os.MkdirAll(older, 0o755); err != nil {
		t.Fatalf("mkdir older: %v", err)
	}
	if err := os.MkdirAll(newer, 0o755); err != nil {
		t.Fatalf("mkdir newer: %v", err)
	}

	manifest := countryimport.ExportManifest{
		ManifestVersion: countryimport.ExportManifestVersion,
		CountryISO2:     CountryISO2,
		SourceSlug:      ptrString(SourceSECEdgar),
		ExportKind:      "source",
		RunID:           "20260607T010000Z-secedgar",
		SchemaVersion:   "test.schema.v1",
		CreatedAt:       time.Date(2026, 6, 7, 1, 0, 1, 0, time.UTC),
		RecordsExported: 2,
	}
	if err := countryimport.SaveExportManifest(filepath.Join(older, "manifest.json"), manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	status, err := SourceStatusFromLatestManifest(root, SourceSECEdgar)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != "exported" {
		t.Fatalf("Status = %q", status.Status)
	}
	if status.LastExportManifestPath != filepath.Join(older, "manifest.json") {
		t.Fatalf("LastExportManifestPath = %q", status.LastExportManifestPath)
	}
}

func ptrString(value string) *string {
	return &value
}
