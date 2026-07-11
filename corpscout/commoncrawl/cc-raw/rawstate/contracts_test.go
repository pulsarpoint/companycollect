package rawstate

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cc-raw/rawstore"
)

const (
	readySHA256     = rawstore.SHA256("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	processedSHA256 = rawstore.SHA256("9999999999999999999999999999999999999999999999999999999999999999")
)

func TestGoldenStateMarkers(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		marker   interface{ Validate() error }
	}{
		{name: "processing", filename: "processing.json", marker: &ProcessingMarker{}},
		{name: "processed", filename: "processed.json", marker: &ProcessedMarker{}},
		{name: "loaded", filename: "loaded.json", marker: &LoadedMarker{}},
		{name: "reclaimed", filename: "reclaimed.json", marker: &ReclaimedMarker{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decodeJSONFile(t, filepath.Join("testdata", test.filename), test.marker)
			if err := test.marker.Validate(); err != nil {
				t.Fatalf("validate %s: %v", test.filename, err)
			}
		})
	}
}

func TestProcessingMarkerLeaseBoundary(t *testing.T) {
	var marker ProcessingMarker
	decodeJSONFile(t, filepath.Join("testdata", "processing.json"), &marker)
	if marker.IsStale(marker.LeaseExpiresAt.Add(-time.Nanosecond)) {
		t.Fatal("lease reported stale before expiry")
	}
	if !marker.IsStale(marker.LeaseExpiresAt) {
		t.Fatal("lease did not report stale at expiry")
	}

	marker.HeartbeatAt = marker.StartedAt.Add(-time.Second)
	if err := marker.Validate(); err == nil {
		t.Fatal("heartbeat before start passed validation")
	}
}

func TestProcessedMarkerMatchesReadyManifest(t *testing.T) {
	var processed ProcessedMarker
	decodeJSONFile(t, filepath.Join("testdata", "processed.json"), &processed)
	var ready rawstore.ReadyManifest
	decodeJSONFile(t, filepath.Join("..", "rawstore", "testdata", "ready.json"), &ready)

	if err := ValidateProcessedAgainstReady(processed, ready, readySHA256); err != nil {
		t.Fatalf("validate processed against ready: %v", err)
	}

	processed.Counts.DownloadedRecords--
	if err := ValidateProcessedAgainstReady(processed, ready, readySHA256); err == nil {
		t.Fatal("mismatched processed counts passed validation")
	}
}

func TestLoadedMarkerMatchesProcessedMarker(t *testing.T) {
	var loaded LoadedMarker
	decodeJSONFile(t, filepath.Join("testdata", "loaded.json"), &loaded)
	var processed ProcessedMarker
	decodeJSONFile(t, filepath.Join("testdata", "processed.json"), &processed)

	if err := ValidateLoadedAgainstProcessed(loaded, processed, processedSHA256); err != nil {
		t.Fatalf("validate loaded against processed: %v", err)
	}

	loaded.SourceRunID = "another-run"
	if err := ValidateLoadedAgainstProcessed(loaded, processed, processedSHA256); err == nil {
		t.Fatal("loaded marker referencing another run passed validation")
	}
}

func TestProcessedMarkerRejectsDuplicateArtifacts(t *testing.T) {
	var marker ProcessedMarker
	decodeJSONFile(t, filepath.Join("testdata", "processed.json"), &marker)
	marker.Outputs = append(marker.Outputs, marker.Outputs[0])
	if err := marker.Validate(); err == nil {
		t.Fatal("duplicate output artifacts passed validation")
	}
}

func TestReclaimedMarkerRequiresDeletedData(t *testing.T) {
	var marker ReclaimedMarker
	decodeJSONFile(t, filepath.Join("testdata", "reclaimed.json"), &marker)
	marker.DeletedObjectCount = 0
	if err := marker.Validate(); err == nil {
		t.Fatal("empty reclamation passed validation")
	}
}

func TestStateKeys(t *testing.T) {
	ready, err := DownloadReadyKey("CC-MAIN-2026-25", "tech25", 7)
	if err != nil {
		t.Fatal(err)
	}
	if ready != "commoncrawl/state/crawl=CC-MAIN-2026-25/selection=tech25/part=007/download/ready.json" {
		t.Fatalf("unexpected ready key %q", ready)
	}

	keys, err := KeysForProcessor("CC-MAIN-2026-25", "tech25", 7, "tech")
	if err != nil {
		t.Fatal(err)
	}
	if keys.Loaded != "commoncrawl/state/crawl=CC-MAIN-2026-25/selection=tech25/part=007/processor=tech/loaded.json" {
		t.Fatalf("unexpected loaded key %q", keys.Loaded)
	}
	if _, err := KeysForProcessor("CC-MAIN-2026-25", "tech25", 7, "../tech"); err == nil {
		t.Fatal("path-like processor passed validation")
	}
}

func decodeJSONFile(t *testing.T, path string, destination any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("%s contains trailing JSON", path)
	}
}
