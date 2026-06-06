package samgoventity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestDownloadWritesNDJSONSnapshotAndMetadata(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "0" {
			http.ServeFile(w, r, "testdata/sam_page.json")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"totalRecords":2,"entityData":[]}`))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:        server.URL,
		APIKey:         "test-key",
		DataDir:        t.TempDir(),
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	if result.RecordsSeen != 2 {
		t.Fatalf("RecordsSeen = %d, want 2", result.RecordsSeen)
	}
	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1", result.PagesDownloaded)
	}

	data, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("snapshot line count = %d, want 2", len(lines))
	}

	var first SamEntityRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first line: %v", err)
	}
	if first.EntityRegistration.UeiSAM != "ABC123DEF456" {
		t.Fatalf("first record UEI = %q, want ABC123DEF456", first.EntityRegistration.UeiSAM)
	}

	sum := sha256.Sum256(data)
	if result.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 mismatch")
	}
	if store.download.TotalResultsReported == nil || *store.download.TotalResultsReported != 2 {
		t.Fatalf("metadata TotalResultsReported = %v, want 2", store.download.TotalResultsReported)
	}
}

func TestDownloadSendsApiKeyViaHeaderNotURL(t *testing.T) {
	var gotKey string
	var rawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Api-Key")
		rawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"totalRecords":0,"entityData":[]}`))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		APIKey:     "super-secret-key",
		DataDir:    t.TempDir(),
		HTTPClient: server.Client(),
	})

	if _, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond}); err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if gotKey != "super-secret-key" {
		t.Fatalf("X-Api-Key header = %q, want super-secret-key", gotKey)
	}
	if strings.Contains(rawQuery, "super-secret-key") || strings.Contains(rawQuery, "api_key") {
		t.Fatalf("api key leaked into URL query: %q", rawQuery)
	}
}

func TestDownloadRequiresApiKey(t *testing.T) {
	source := NewSource(Config{
		BaseURL: "https://example.invalid",
		DataDir: t.TempDir(),
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if !countryimport.IsKind(err, countryimport.ErrorKindInvalidConfig) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindInvalidConfig, err)
	}
}

func TestDownloadPaginatesUntilTotalRecords(t *testing.T) {
	var pages []string
	entity := func(uei string) string {
		return `{"entityRegistration":{"ueiSAM":"` + uei + `","legalBusinessName":"ORG ` + uei + `","registrationStatus":"Active"}}`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages = append(pages, r.URL.Query().Get("page"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("page") {
		case "0":
			_, _ = w.Write([]byte(`{"totalRecords":3,"entityData":[` + entity("AAA000000001") + `,` + entity("AAA000000002") + `]}`))
		case "1":
			_, _ = w.Write([]byte(`{"totalRecords":3,"entityData":[` + entity("AAA000000003") + `]}`))
		default:
			_, _ = w.Write([]byte(`{"totalRecords":3,"entityData":[]}`))
		}
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		APIKey:     "k",
		DataDir:    t.TempDir(),
		PageSize:   2,
		HTTPClient: server.Client(),
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if result.RecordsSeen != 3 {
		t.Fatalf("RecordsSeen = %d, want 3", result.RecordsSeen)
	}
	// page 0 (full) then page 1 (reaches totalRecords) — stops without page 2.
	if len(pages) != 2 || pages[0] != "0" || pages[1] != "1" {
		t.Fatalf("pages = %v, want [0 1]", pages)
	}
}

func TestDownloadReturnsHTTPStatusKindOnForbidden(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:       server.URL,
		APIKey:        "k",
		DataDir:       t.TempDir(),
		HTTPClient:    server.Client(),
		MetadataStore: store,
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if !countryimport.IsKind(err, countryimport.ErrorKindHTTPStatus) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindHTTPStatus, err)
	}
	if store.downloadCalls != 0 {
		t.Fatalf("SaveDownload calls = %d, want 0", store.downloadCalls)
	}
}

type recordingMetadataStore struct {
	downloadCalls int
	download      countryimport.DownloadMetadata
}

func (s *recordingMetadataStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	s.downloadCalls++
	s.download = metadata
	return nil
}

func (s *recordingMetadataStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	return nil
}
