package countrydata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUnitedStatesSECEDGARImporterRunUsesSharedSourceMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},"1":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"}}`))
	}))
	t.Cleanup(server.Close)

	importer := UnitedStatesSECEDGARImporter{HTTPClient: server.Client()}

	result, err := importer.Run(context.Background(), UnitedStatesSECEDGARImportInput{
		DownloadURL:    server.URL,
		DataDir:        t.TempDir(),
		ChunkSize:      1,
		RequestTimeout: time.Second,
		UserAgent:      "corpscout-test/1.0 (contact@example.com)",
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Download.RecordsSeen != 2 {
		t.Fatalf("Download.RecordsSeen = %d, want 2", result.Download.RecordsSeen)
	}
	if result.Process.RecordsProcessed != 2 {
		t.Fatalf("Process.RecordsProcessed = %d, want 2", result.Process.RecordsProcessed)
	}
}
