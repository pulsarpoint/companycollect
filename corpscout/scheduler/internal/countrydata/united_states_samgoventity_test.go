package countrydata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUnitedStatesSamGovEntityImporterRunUsesSharedSourceMethods(t *testing.T) {
	var gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Api-Key")
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("page") {
		case "0":
			_, _ = w.Write([]byte(`{"totalRecords":1,"entityData":[{"entityRegistration":{"ueiSAM":"ABC123DEF456","legalBusinessName":"EXAMPLE ANALYTICS LLC","registrationStatus":"Active"}}]}`))
		default:
			_, _ = w.Write([]byte(`{"totalRecords":1,"entityData":[]}`))
		}
	}))
	t.Cleanup(server.Close)

	importer := UnitedStatesSamGovEntityImporter{HTTPClient: server.Client()}

	result, err := importer.Run(context.Background(), UnitedStatesSamGovEntityImportInput{
		BaseURL:        server.URL,
		APIKey:         "header-key",
		DataDir:        t.TempDir(),
		PageSize:       1,
		ChunkSize:      1,
		PageDelay:      time.Nanosecond,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if gotKey != "header-key" {
		t.Fatalf("X-Api-Key header = %q, want header-key", gotKey)
	}
	if result.Download.RecordsSeen != 1 {
		t.Fatalf("Download.RecordsSeen = %d, want 1", result.Download.RecordsSeen)
	}
	if result.Process.RecordsProcessed != 1 {
		t.Fatalf("Process.RecordsProcessed = %d, want 1", result.Process.RecordsProcessed)
	}
	if result.Process.SnapshotPath != result.Download.SnapshotPath {
		t.Fatalf("process snapshot %q != download snapshot %q", result.Process.SnapshotPath, result.Download.SnapshotPath)
	}
}
