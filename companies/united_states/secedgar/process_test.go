package secedgar

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestProcessReadsSnapshotInChunksUsesLatestAndHonorsLimit(t *testing.T) {
	dataDir := t.TempDir()
	olderPath := filepath.Join(dataDir, "snapshots", "older.json")
	newerPath := filepath.Join(dataDir, "snapshots", "newer.json")
	writeSECTestFile(t, olderPath, []byte(`{"0":{"cik_str":1,"ticker":"OLD","title":"Old Inc."}}`))
	writeSECTestFile(t, newerPath, []byte(`{
		"2":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."},
		"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple   Inc."},
		"10":{"cik_str":1750,"ticker":"AIR","title":"AAR Corp."}
	}`))
	olderTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	newerTime := olderTime.Add(time.Hour)
	if err := os.Chtimes(olderPath, olderTime, olderTime); err != nil {
		t.Fatalf("set older snapshot time: %v", err)
	}
	if err := os.Chtimes(newerPath, newerTime, newerTime); err != nil {
		t.Fatalf("set newer snapshot time: %v", err)
	}

	metadataStore := &secProcessRecordingMetadataStore{}
	var chunkSizes []int
	var stored []CompanyTickerRecord
	source := NewSource(Config{
		DataDir:       dataDir,
		MetadataStore: metadataStore,
	})
	source.StoreFunc = func(ctx context.Context, records []CompanyTickerRecord) (countryimport.StoreResult, error) {
		chunkSizes = append(chunkSizes, len(records))
		stored = append(stored, records...)
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	result, err := source.Process(context.Background(), countryimport.ProcessOptions{
		DataDir:   dataDir,
		ChunkSize: 2,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if result.SnapshotPath != newerPath {
		t.Fatalf("SnapshotPath = %q, want %q", result.SnapshotPath, newerPath)
	}
	if result.RecordsSeen != 3 {
		t.Fatalf("RecordsSeen = %d, want 3", result.RecordsSeen)
	}
	if result.RecordsProcessed != 2 || result.RecordsStored != 2 {
		t.Fatalf("processed/stored = %d/%d, want 2/2", result.RecordsProcessed, result.RecordsStored)
	}
	if result.ChunksProcessed != 1 {
		t.Fatalf("ChunksProcessed = %d, want 1", result.ChunksProcessed)
	}
	if len(chunkSizes) != 1 || chunkSizes[0] != 2 {
		t.Fatalf("chunk sizes = %#v, want [2]", chunkSizes)
	}
	if len(stored) != 2 || stored[0].Ticker != "AAPL" || stored[1].Ticker != "MSFT" {
		t.Fatalf("stored records = %#v, want first two sorted records", stored)
	}
	if metadataStore.processCalls != 1 {
		t.Fatalf("SaveProcess calls = %d, want 1", metadataStore.processCalls)
	}
	if metadataStore.process.SnapshotPath != newerPath {
		t.Fatalf("metadata SnapshotPath = %q, want %q", metadataStore.process.SnapshotPath, newerPath)
	}
	if metadataStore.process.RecordsProcessed != result.RecordsProcessed {
		t.Fatalf("metadata RecordsProcessed = %d, want %d", metadataStore.process.RecordsProcessed, result.RecordsProcessed)
	}
	if source.latestProcess == nil || source.latestProcess.SnapshotPath != newerPath {
		t.Fatalf("latestProcess = %#v, want newer snapshot", source.latestProcess)
	}
}

func TestProcessWithoutSnapshotReturnsNoSnapshotKind(t *testing.T) {
	source := NewSource(Config{DataDir: t.TempDir()})

	_, err := source.Process(context.Background(), countryimport.ProcessOptions{})
	if err == nil {
		t.Fatal("Process returned nil error, want no_snapshot")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindNoSnapshot) {
		t.Fatalf("Process error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindNoSnapshot, err)
	}
}

func TestProcessBadJSONReturnsRemoteDecodeKind(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := filepath.Join(dataDir, "snapshots", "bad.json")
	writeSECTestFile(t, snapshotPath, []byte(`{"0":{"cik_str":`))
	source := NewSource(Config{DataDir: dataDir})

	result, err := source.Process(context.Background(), countryimport.ProcessOptions{SnapshotPath: snapshotPath})
	if err == nil {
		t.Fatal("Process returned nil error, want remote_decode")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
		t.Fatalf("Process error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindRemoteDecode, err)
	}
	if result.DecodeErrors != 1 {
		t.Fatalf("DecodeErrors = %d, want 1", result.DecodeErrors)
	}
}

type secProcessRecordingMetadataStore struct {
	processCalls int
	process      countryimport.ProcessMetadata
}

func (s *secProcessRecordingMetadataStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	return nil
}

func (s *secProcessRecordingMetadataStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	s.processCalls++
	s.process = metadata
	return nil
}

func writeSECTestFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
