package countryimport

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExportManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	createdAt := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	manifest := ExportManifest{
		ManifestVersion: "countrydata.export.v1",
		CountryISO2:     "FI",
		SourceSlug:      ptrString("prhytj"),
		ExportKind:      "source",
		RunID:           "20260607T120000Z-prhytj",
		SchemaVersion:   "finland.prhytj.source.v1",
		CreatedAt:       createdAt,
		Files: []ExportFile{{
			Name:       "companies",
			Path:       "companies.parquet",
			RowCount:   1,
			SHA256:     "abcdef",
			SchemaHash: "schemahash",
		}},
		RecordsSeen:     1,
		RecordsExported: 1,
	}

	if err := SaveExportManifest(path, manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	loaded, err := LoadExportManifest(path)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.ManifestVersion != manifest.ManifestVersion || loaded.RunID != manifest.RunID {
		t.Fatalf("loaded manifest mismatch: %#v", loaded)
	}
	if loaded.SourceSlug == nil || *loaded.SourceSlug != "prhytj" {
		t.Fatalf("SourceSlug = %#v, want prhytj", loaded.SourceSlug)
	}
}

func TestHashFileSHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	hash, size, err := HashFileSHA256(path)
	if err != nil {
		t.Fatalf("hash file: %v", err)
	}
	if size != 5 {
		t.Fatalf("size = %d, want 5", size)
	}
	if hash != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Fatalf("hash = %q", hash)
	}
}

func ptrString(value string) *string {
	return &value
}
