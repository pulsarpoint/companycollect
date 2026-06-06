package prhytj

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

func TestDownloadWritesPaginatedNDJSONSnapshotAndMetadata(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("page") {
		case "", "1":
			http.ServeFile(w, r, "testdata/prh_page_1.json")
		case "2":
			http.ServeFile(w, r, "testdata/prh_page_2.json")
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"companies":[]}`))
		}
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:        server.URL,
		DataDir:        t.TempDir(),
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:  3,
		PageDelay: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	if result.PagesDownloaded != 2 {
		t.Fatalf("PagesDownloaded = %d, want 2", result.PagesDownloaded)
	}
	if result.RecordsSeen != 3 {
		t.Fatalf("RecordsSeen = %d, want 3", result.RecordsSeen)
	}

	if _, err := os.Stat(result.SnapshotPath); err != nil {
		t.Fatalf("snapshot file does not exist: %v", err)
	}

	data, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("snapshot line count = %d, want 3", len(lines))
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
	if store.download.PagesDownloaded != result.PagesDownloaded {
		t.Fatalf("metadata PagesDownloaded = %d, want %d", store.download.PagesDownloaded, result.PagesDownloaded)
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

func TestDownloadReturnsRemoteDecodeWhenCompaniesIsMissingOrNull(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{}`},
		{name: "null", body: `{"companies":null}`},
		{name: "not array", body: `{"companies":{}}`},
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
				BaseURL:       server.URL,
				DataDir:       t.TempDir(),
				HTTPClient:    server.Client(),
				MetadataStore: store,
			})

			_, err := source.Download(context.Background(), countryimport.DownloadOptions{
				MaxPages:  1,
				PageDelay: time.Nanosecond,
			})
			if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
				t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindRemoteDecode, err)
			}
			if store.downloadCalls != 0 {
				t.Fatalf("SaveDownload calls = %d, want 0", store.downloadCalls)
			}
		})
	}
}

func TestDownloadReturnsRemoteDecodeForUnexpectedEarlyEmptyPage(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("page") {
		case "", "1":
			_, _ = w.Write([]byte(`{"totalResults":2,"companies":[{"businessId":{"value":"0100130-4"}}]}`))
		default:
			_, _ = w.Write([]byte(`{"companies":[]}`))
		}
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:       server.URL,
		DataDir:       t.TempDir(),
		HTTPClient:    server.Client(),
		MetadataStore: store,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:  2,
		PageDelay: time.Nanosecond,
	})
	if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindRemoteDecode, err)
	}
	if result.SnapshotPath != "" {
		if _, statErr := os.Stat(result.SnapshotPath); !os.IsNotExist(statErr) {
			t.Fatalf("incomplete snapshot path still exists or stat failed unexpectedly: %v", statErr)
		}
	}
	if store.downloadCalls != 0 {
		t.Fatalf("SaveDownload calls = %d, want 0", store.downloadCalls)
	}
}

func TestDownloadRequestTimeoutOverrideControlsDefaultClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"companies":[]}`))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:        server.URL,
		DataDir:        t.TempDir(),
		RequestTimeout: time.Nanosecond,
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:       1,
		PageDelay:      time.Nanosecond,
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
