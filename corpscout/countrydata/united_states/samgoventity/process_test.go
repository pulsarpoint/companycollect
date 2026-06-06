package samgoventity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestProcessReadsSnapshotInChunksAndContinuesAfterBadLine(t *testing.T) {
	metadataStore := &processRecordingMetadataStore{}
	var chunkSizes []int
	source := NewSource(Config{
		APIKey:        "k",
		DataDir:       t.TempDir(),
		MetadataStore: metadataStore,
	})
	source.StoreFunc = func(ctx context.Context, records []SamEntityRecord) (countryimport.StoreResult, error) {
		chunkSizes = append(chunkSizes, len(records))
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	snapshotPath := filepath.Join("testdata", "sam_snapshot_mixed.ndjson")
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
}

func TestProcessPreservesNestedFields(t *testing.T) {
	var captured []SamEntityRecord
	source := NewSource(Config{APIKey: "k", DataDir: t.TempDir()})
	source.StoreFunc = func(ctx context.Context, records []SamEntityRecord) (countryimport.StoreResult, error) {
		captured = append(captured, records...)
		return countryimport.StoreResult{RecordsReceived: int64(len(records)), RecordsStored: int64(len(records))}, nil
	}

	_, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: filepath.Join("testdata", "sam_snapshot_mixed.ndjson"),
		ChunkSize:    100,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if len(captured) != 3 {
		t.Fatalf("captured records = %d, want 3", len(captured))
	}
	if captured[0].EntityRegistration.UeiSAM != "ABC123DEF456" {
		t.Fatalf("first UEI = %q", captured[0].EntityRegistration.UeiSAM)
	}
	if captured[0].Assertions.GoodsAndServices.PrimaryNaics != "541511" {
		t.Fatalf("first primaryNaics = %q, want 541511", captured[0].Assertions.GoodsAndServices.PrimaryNaics)
	}
	if captured[0].CoreData.GeneralInformation.StateOfIncorporationCode != "CO" {
		t.Fatalf("first state of incorporation = %q, want CO", captured[0].CoreData.GeneralInformation.StateOfIncorporationCode)
	}
}

func TestProcessWithoutSnapshotReturnsNoSnapshotKind(t *testing.T) {
	source := NewSource(Config{APIKey: "k", DataDir: t.TempDir()})

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
	source := NewSource(Config{APIKey: "k", DataDir: t.TempDir()})
	source.StoreFunc = func(ctx context.Context, records []SamEntityRecord) (countryimport.StoreResult, error) {
		storeCalls++
		cancel()
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	result, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: filepath.Join("testdata", "sam_snapshot_mixed.ndjson"),
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

func TestProcessSetsRawPayloadAndPayloadHash(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.ndjson")
	line := `{"entityRegistration":{"ueiSAM":"ABC123DEF456","legalBusinessName":"Example Corp","registrationStatus":"Active"}}`
	if err := os.WriteFile(snapshotPath, []byte(line+"\n"), 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	var stored []SamEntityRecord
	source := NewSource(Config{APIKey: "test-key"})
	source.StoreFunc = func(ctx context.Context, records []SamEntityRecord) (countryimport.StoreResult, error) {
		stored = append(stored, records...)
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	result, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: snapshotPath,
		ChunkSize:    1,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if result.RecordsProcessed != 1 {
		t.Fatalf("RecordsProcessed = %d, want 1", result.RecordsProcessed)
	}
	if len(stored) != 1 {
		t.Fatalf("stored records = %d, want 1", len(stored))
	}
	if got := string(stored[0].RawPayload); got != line {
		t.Fatalf("RawPayload = %q, want %q", got, line)
	}
	sum := sha256.Sum256([]byte(line))
	if want := hex.EncodeToString(sum[:]); stored[0].PayloadHash != want {
		t.Fatalf("PayloadHash = %q, want %q", stored[0].PayloadHash, want)
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
