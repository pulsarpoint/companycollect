package prhytj

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestWriteParquetRowsWritesReadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "companies.parquet")
	rows := []CompanyExportRow{{
		CountryISO2:   "FI",
		SourceSlug:    SourceSlug,
		SourceRunID:   "run-1",
		BusinessID:    "0100130-4",
		LegalName:     "Dynava Oy",
		SchemaVersion: SourceExportSchemaVersion,
	}}

	if err := WriteParquetRows(path, rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat parquet: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("parquet file is empty")
	}
	handle, err := os.Open(path)
	if err != nil {
		t.Fatalf("open parquet handle: %v", err)
	}
	defer handle.Close()
	file, err := parquet.OpenFile(handle, info.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	if file.NumRows() != 1 {
		t.Fatalf("NumRows = %d, want 1", file.NumRows())
	}
}

func TestWriteParquetRowsDoesNotUseDeterministicTempPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "companies.parquet")
	deterministicTempPath := path + ".tmp"
	sentinel := []byte("existing temp file")
	if err := os.WriteFile(deterministicTempPath, sentinel, 0o644); err != nil {
		t.Fatalf("write sentinel temp file: %v", err)
	}
	rows := []CompanyExportRow{{
		CountryISO2:   "FI",
		SourceSlug:    SourceSlug,
		SourceRunID:   "run-1",
		BusinessID:    "0100130-4",
		LegalName:     "Dynava Oy",
		SchemaVersion: SourceExportSchemaVersion,
	}}

	if err := WriteParquetRows(path, rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	actual, err := os.ReadFile(deterministicTempPath)
	if err != nil {
		t.Fatalf("read sentinel temp file: %v", err)
	}
	if string(actual) != string(sentinel) {
		t.Fatalf("sentinel temp file = %q, want %q", actual, sentinel)
	}
}
