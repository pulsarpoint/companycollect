package countrydata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFinlandPRHYTJImporterRunUsesSharedSourceMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Query().Get("page") {
		case "", "1":
			_, _ = w.Write([]byte(`{"companies":[{"businessId":{"type":"Y-tunnus","value":"0112038-9"},"names":[{"name":"Testi Oy","type":"name","version":1}],"registrationDate":"2024-01-01","lastModified":"2024-01-02T03:04:05Z"}]}`))
		case "2":
			_, _ = w.Write([]byte(`{"companies":[]}`))
		default:
			t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	t.Cleanup(server.Close)

	importer := FinlandPRHYTJImporter{HTTPClient: server.Client()}

	result, err := importer.Run(context.Background(), FinlandPRHYTJImportInput{
		BaseURL:   server.URL,
		DataDir:   t.TempDir(),
		MaxPages:  2,
		ChunkSize: 1,
		PageDelay: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Download.RecordsSeen != 1 {
		t.Fatalf("Download.RecordsSeen = %d, want 1", result.Download.RecordsSeen)
	}
	if result.Process.RecordsProcessed != 1 {
		t.Fatalf("Process.RecordsProcessed = %d, want 1", result.Process.RecordsProcessed)
	}
}
