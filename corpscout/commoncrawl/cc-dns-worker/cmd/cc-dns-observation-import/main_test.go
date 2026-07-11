package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestReadRecordBatchMapsScannerRows(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "scan.db")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE scan_records (
		scan_id TEXT NOT NULL, root_domain TEXT NOT NULL, name TEXT NOT NULL,
		record_type TEXT NOT NULL, slot TEXT DEFAULT '', value TEXT NOT NULL,
		ttl INTEGER DEFAULT 0, priority INTEGER DEFAULT 0, rcode TEXT DEFAULT '',
		source_run_id TEXT DEFAULT '', resolved_at TEXT DEFAULT '');
		INSERT INTO scan_records VALUES
		('2026-07-07', 'example.com', '_dmarc.example.com', 'TXT', 'dmarc', 'v=DMARC1',
		 300, 0, 'NOERROR', '2026-07-07', '2026-07-07T12:34:56.123456789Z');`); err != nil {
		t.Fatal(err)
	}
	_ = database.Close()

	readOnly, err := openSQLiteReadOnly(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	records, err := readRecordBatch(context.Background(), readOnly, 0, 10, "2026-07-07")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	record := records[0]
	if record.rootDomain != "example.com" || record.recordType != "TXT" || record.slot != "dmarc" {
		t.Errorf("record = %+v", record)
	}
	wantTime := time.Date(2026, 7, 7, 12, 34, 56, 123456789, time.UTC)
	if !record.observedAt.Equal(wantTime) {
		t.Errorf("observed_at = %v, want %v", record.observedAt, wantTime)
	}
	if want := importVersionBase.Add(time.Millisecond); !record.loadedAt.Equal(want) {
		t.Errorf("loaded_at = %v, want %v", record.loadedAt, want)
	}
}

func TestReadRecordBatchRejectsDifferentScan(t *testing.T) {
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE scan_records (
		scan_id TEXT, root_domain TEXT, name TEXT, record_type TEXT, slot TEXT, value TEXT,
		ttl INTEGER, priority INTEGER, rcode TEXT, source_run_id TEXT, resolved_at TEXT);
		INSERT INTO scan_records VALUES
		('wrong', 'example.com', 'example.com', 'A', '@', '1.2.3.4', 60, 0,
		 'NOERROR', 'wrong', '2026-07-07T00:00:00Z');`); err != nil {
		t.Fatal(err)
	}
	if _, err := readRecordBatch(context.Background(), database, 0, 10, "2026-07-07"); err == nil {
		t.Fatal("expected scan mismatch error")
	}
}
