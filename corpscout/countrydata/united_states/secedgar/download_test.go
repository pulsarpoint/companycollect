package secedgar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
		http.ServeFile(w, r, "testdata/company_tickers.json")
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURL:    server.URL,
		DataDir:        t.TempDir(),
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1", result.PagesDownloaded)
	}
	if result.RecordsSeen != 4 {
		t.Fatalf("RecordsSeen = %d, want 4", result.RecordsSeen)
	}

	data, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("snapshot line count = %d, want 4", len(lines))
	}
	// Index keys must be flattened away: each line is a bare company object and
	// the first record (index "0") is NVIDIA.
	if !strings.Contains(lines[0], `"cik_str":1045810`) || !strings.Contains(lines[0], `"NVIDIA CORP"`) {
		t.Fatalf("first snapshot line = %q, want NVIDIA record", lines[0])
	}

	sum := sha256.Sum256(data)
	expectedSHA256 := hex.EncodeToString(sum[:])
	if result.SHA256 != expectedSHA256 {
		t.Fatalf("SHA256 = %q, want %q", result.SHA256, expectedSHA256)
	}

	if store.downloadCalls != 1 {
		t.Fatalf("SaveDownload calls = %d, want 1", store.downloadCalls)
	}
	if store.download.SourceSlug != SourceSlug {
		t.Fatalf("metadata SourceSlug = %q, want %q", store.download.SourceSlug, SourceSlug)
	}
	if store.download.RecordsSeen != result.RecordsSeen {
		t.Fatalf("metadata RecordsSeen = %d, want %d", store.download.RecordsSeen, result.RecordsSeen)
	}
	if store.download.SnapshotPath != result.SnapshotPath {
		t.Fatalf("metadata SnapshotPath = %q, want %q", store.download.SnapshotPath, result.SnapshotPath)
	}
	if store.download.SHA256 != result.SHA256 {
		t.Fatalf("metadata SHA256 = %q, want %q", store.download.SHA256, result.SHA256)
	}
}

func TestDownloadSendsConfiguredUserAgent(t *testing.T) {
	var gotUserAgent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserAgent = r.Header.Get("User-Agent")
		http.ServeFile(w, r, "testdata/company_tickers.json")
	}))
	t.Cleanup(server.Close)

	const userAgent = "corpscout-test/1.0 (contact@example.com)"
	source := NewSource(Config{
		DownloadURL: server.URL,
		DataDir:     t.TempDir(),
		HTTPClient:  server.Client(),
		UserAgent:   userAgent,
	})

	if _, err := source.Download(context.Background(), countryimport.DownloadOptions{}); err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if gotUserAgent != userAgent {
		t.Fatalf("User-Agent = %q, want %q", gotUserAgent, userAgent)
	}
}

func TestDownloadReturnsRemoteDecodeForNonObjectBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "array", body: `[]`},
		{name: "null", body: `null`},
		{name: "string", body: `"nope"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &recordingMetadataStore{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(server.Close)

			source := NewSource(Config{
				DownloadURL:   server.URL,
				DataDir:       t.TempDir(),
				HTTPClient:    server.Client(),
				MetadataStore: store,
			})

			_, err := source.Download(context.Background(), countryimport.DownloadOptions{})
			if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
				t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindRemoteDecode, err)
			}
			if store.downloadCalls != 0 {
				t.Fatalf("SaveDownload calls = %d, want 0", store.downloadCalls)
			}
		})
	}
}

func TestDownloadReturnsHTTPStatusKindOnForbidden(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURL:   server.URL,
		DataDir:       t.TempDir(),
		HTTPClient:    server.Client(),
		MetadataStore: store,
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindHTTPStatus) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindHTTPStatus, err)
	}
	if store.downloadCalls != 0 {
		t.Fatalf("SaveDownload calls = %d, want 0", store.downloadCalls)
	}
}

func TestDownloadRequestTimeoutOverrideControlsDefaultClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		http.ServeFile(w, r, "testdata/company_tickers.json")
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURL:    server.URL,
		DataDir:        t.TempDir(),
		RequestTimeout: time.Nanosecond,
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
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
