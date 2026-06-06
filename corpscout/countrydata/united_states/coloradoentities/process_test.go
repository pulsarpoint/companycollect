package coloradoentities

import (
	"context"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestProcessReadsSnapshotInChunksAndContinuesAfterBadLine(t *testing.T) {
	metadataStore := &processRecordingMetadataStore{}
	var chunkSizes []int
	source := NewSource(Config{
		DataDir:       t.TempDir(),
		MetadataStore: metadataStore,
	})
	source.StoreFunc = func(ctx context.Context, records []ColoradoEntityRecord) (countryimport.StoreResult, error) {
		chunkSizes = append(chunkSizes, len(records))
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	snapshotPath := filepath.Join("testdata", "co_snapshot_mixed.ndjson")
	result, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: snapshotPath,
		ChunkSize:    2,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if result.RecordsSeen != 4 {
		t.Fatalf("RecordsSeen = %d, want 4", result.RecordsSeen)
	}
	if result.RecordsProcessed != 3 {
		t.Fatalf("RecordsProcessed = %d, want 3", result.RecordsProcessed)
	}
	if result.DecodeErrors != 1 {
		t.Fatalf("DecodeErrors = %d, want 1", result.DecodeErrors)
	}
	if result.RecordsStored != 3 {
		t.Fatalf("RecordsStored = %d, want 3", result.RecordsStored)
	}
	if len(chunkSizes) != 2 || chunkSizes[0] != 2 || chunkSizes[1] != 1 {
		t.Fatalf("chunk sizes = %#v, want [2 1]", chunkSizes)
	}

	if metadataStore.processCalls != 1 {
		t.Fatalf("SaveProcess calls = %d, want 1", metadataStore.processCalls)
	}
	if metadataStore.process.RecordsProcessed != result.RecordsProcessed {
		t.Fatalf("metadata RecordsProcessed = %d, want %d", metadataStore.process.RecordsProcessed, result.RecordsProcessed)
	}
}

func TestProcessPreservesParsedFields(t *testing.T) {
	var captured []ColoradoEntityRecord
	source := NewSource(Config{DataDir: t.TempDir()})
	source.StoreFunc = func(ctx context.Context, records []ColoradoEntityRecord) (countryimport.StoreResult, error) {
		captured = append(captured, records...)
		return countryimport.StoreResult{RecordsReceived: int64(len(records)), RecordsStored: int64(len(records))}, nil
	}

	_, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: filepath.Join("testdata", "co_snapshot_mixed.ndjson"),
		ChunkSize:    100,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if len(captured) != 3 {
		t.Fatalf("captured records = %d, want 3", len(captured))
	}
	if captured[0].EntityID != "20251665680" || captured[0].AgentLastName != "DENG" {
		t.Fatalf("first record = %#v", captured[0])
	}
	// Foreign entity retains its out-of-state jurisdiction and org agent.
	if captured[2].JurisdictonOfFormation != "TX" || captured[2].AgentOrganizationName != "C T CORPORATION SYSTEM" {
		t.Fatalf("third record = %#v", captured[2])
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

func TestProcessStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var storeCalls int
	source := NewSource(Config{DataDir: t.TempDir()})
	source.StoreFunc = func(ctx context.Context, records []ColoradoEntityRecord) (countryimport.StoreResult, error) {
		storeCalls++
		cancel()
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	result, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: filepath.Join("testdata", "co_snapshot_mixed.ndjson"),
		ChunkSize:    1,
	})
	if err == nil {
		t.Fatal("Process returned nil error, want cancellation")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindTimeout) {
		t.Fatalf("Process error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindTimeout, err)
	}
	if storeCalls != 1 {
		t.Fatalf("Store calls = %d, want 1", storeCalls)
	}
	if result.RecordsProcessed != 1 {
		t.Fatalf("RecordsProcessed = %d, want 1", result.RecordsProcessed)
	}
}

type processRecordingMetadataStore struct {
	processCalls int
	process      countryimport.ProcessMetadata
}

func (s *processRecordingMetadataStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	return nil
}

func (s *processRecordingMetadataStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	s.processCalls++
	s.process = metadata
	return nil
}
