package coloradoentities

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func copyFixtureSnapshot(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "colorado_entities_sample.ndjson"))
	if err != nil {
		t.Fatalf("read NDJSON fixture: %v", err)
	}
	writeTestFile(t, path, data)
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

func TestProcessStreamsChunksSkipsBadLineAndUsesLatest(t *testing.T) {
	dataDir := t.TempDir()
	olderPath := filepath.Join(dataDir, "snapshots", "older.ndjson")
	newerPath := filepath.Join(dataDir, "snapshots", "newer.ndjson")
	writeTestFile(t, olderPath, []byte(`{"entityid":"00000000000","entityname":"OLD"}`+"\n"))
	copyFixtureSnapshot(t, newerPath)

	olderTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newerTime := olderTime.Add(time.Hour)
	if err := os.Chtimes(olderPath, olderTime, olderTime); err != nil {
		t.Fatalf("chtimes older: %v", err)
	}
	if err := os.Chtimes(newerPath, newerTime, newerTime); err != nil {
		t.Fatalf("chtimes newer: %v", err)
	}

	var chunkSizes []int
	source := NewSource(Config{DataDir: dataDir})
	source.storeFunc = func(ctx context.Context, records []ColoradoEntityRecord) (countryimport.StoreResult, error) {
		chunkSizes = append(chunkSizes, len(records))
		return countryimport.StoreResult{RecordsReceived: int64(len(records)), RecordsStored: int64(len(records))}, nil
	}

	result, err := source.Process(context.Background(), countryimport.ProcessOptions{DataDir: dataDir, ChunkSize: 2})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if result.SnapshotPath != newerPath {
		t.Fatalf("SnapshotPath = %q, want %q", result.SnapshotPath, newerPath)
	}
	if result.RecordsSeen != 4 || result.RecordsProcessed != 3 || result.DecodeErrors != 1 {
		t.Fatalf("counts = seen %d processed %d decode %d, want 4/3/1", result.RecordsSeen, result.RecordsProcessed, result.DecodeErrors)
	}
	if len(chunkSizes) != 2 || chunkSizes[0] != 2 || chunkSizes[1] != 1 {
		t.Fatalf("chunk sizes = %#v, want [2 1]", chunkSizes)
	}
}

func TestProcessHonorsLimit(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "snap.ndjson")
	copyFixtureSnapshot(t, snapshotPath)

	source := NewSource(Config{DataDir: dataDir})
	result, err := source.Process(context.Background(), countryimport.ProcessOptions{SnapshotPath: snapshotPath, Limit: 1})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if result.RecordsProcessed != 1 {
		t.Fatalf("RecordsProcessed = %d, want 1", result.RecordsProcessed)
	}
}

func TestProcessWithoutSnapshotReturnsNoSnapshot(t *testing.T) {
	source := NewSource(Config{DataDir: t.TempDir()})
	_, err := source.Process(context.Background(), countryimport.ProcessOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindNoSnapshot) {
		t.Fatalf("Process error kind = %v, want no_snapshot; err=%v", countryimport.Classify(err), err)
	}
}

func TestProcessRespectsCanceledContext(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "snap.ndjson")
	copyFixtureSnapshot(t, snapshotPath)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := NewSource(Config{DataDir: dataDir})
	_, err := source.Process(ctx, countryimport.ProcessOptions{SnapshotPath: snapshotPath})
	if !countryimport.IsKind(err, countryimport.ErrorKindTimeout) {
		t.Fatalf("Process error kind = %v, want timeout; err=%v", countryimport.Classify(err), err)
	}
}
