package secedgar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestDownloadWritesSnapshotAndMetadata(t *testing.T) {
	payload := []byte(`{"1":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."},"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}`)
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		DownloadURL:    server.URL,
		DataDir:        dataDir,
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	if result.SourceSlug != SourceSlug {
		t.Fatalf("SourceSlug = %q, want %q", result.SourceSlug, SourceSlug)
	}
	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1", result.PagesDownloaded)
	}
	if result.RecordsSeen != 2 {
		t.Fatalf("RecordsSeen = %d, want 2", result.RecordsSeen)
	}
	if !strings.HasPrefix(filepath.Base(result.SnapshotPath), "sec_edgar_company_tickers_") {
		t.Fatalf("SnapshotPath basename = %q, want SEC EDGAR prefix", filepath.Base(result.SnapshotPath))
	}
	if filepath.Dir(result.SnapshotPath) != filepath.Join(dataDir, "snapshots") {
		t.Fatalf("SnapshotPath dir = %q, want snapshots dir", filepath.Dir(result.SnapshotPath))
	}

	data, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("snapshot payload = %q, want %q", string(data), string(payload))
	}

	sum := sha256.Sum256(payload)
	expectedSHA256 := hex.EncodeToString(sum[:])
	if result.BytesDownloaded != int64(len(payload)) {
		t.Fatalf("BytesDownloaded = %d, want %d", result.BytesDownloaded, len(payload))
	}
	if result.SHA256 != expectedSHA256 {
		t.Fatalf("SHA256 = %q, want %q", result.SHA256, expectedSHA256)
	}

	if store.downloadCalls != 1 {
		t.Fatalf("SaveDownload calls = %d, want 1", store.downloadCalls)
	}
	if source.latestDownload == nil {
		t.Fatal("latestDownload is nil")
	}
	if store.download.SourceSlug != SourceSlug {
		t.Fatalf("metadata SourceSlug = %q, want %q", store.download.SourceSlug, SourceSlug)
	}
	if store.download.SourceName != SourceName {
		t.Fatalf("metadata SourceName = %q, want %q", store.download.SourceName, SourceName)
	}
	if store.download.BaseURL != server.URL {
		t.Fatalf("metadata BaseURL = %q, want %q", store.download.BaseURL, server.URL)
	}
	if store.download.SnapshotPath != result.SnapshotPath {
		t.Fatalf("metadata SnapshotPath = %q, want %q", store.download.SnapshotPath, result.SnapshotPath)
	}
	if store.download.BytesDownloaded != result.BytesDownloaded {
		t.Fatalf("metadata BytesDownloaded = %d, want %d", store.download.BytesDownloaded, result.BytesDownloaded)
	}
	if store.download.RecordsSeen != result.RecordsSeen {
		t.Fatalf("metadata RecordsSeen = %d, want %d", store.download.RecordsSeen, result.RecordsSeen)
	}
	if store.download.PagesDownloaded != result.PagesDownloaded {
		t.Fatalf("metadata PagesDownloaded = %d, want %d", store.download.PagesDownloaded, result.PagesDownloaded)
	}
	if store.download.SHA256 != result.SHA256 {
		t.Fatalf("metadata SHA256 = %q, want %q", store.download.SHA256, result.SHA256)
	}
	if store.download.HTTPStatuses["download"] != http.StatusOK {
		t.Fatalf("metadata HTTPStatuses[download] = %d, want %d", store.download.HTTPStatuses["download"], http.StatusOK)
	}
	if store.download.Attribution == "" {
		t.Fatal("metadata Attribution is empty")
	}
}

func TestDownloadSendsUserAgent(t *testing.T) {
	var gotUserAgent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserAgent = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}`))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURL: server.URL,
		DataDir:     t.TempDir(),
		HTTPClient:  server.Client(),
		UserAgent:   "configured-agent",
	})

	if _, err := source.Download(context.Background(), countryimport.DownloadOptions{UserAgent: "override-agent"}); err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if gotUserAgent != "override-agent" {
		t.Fatalf("User-Agent = %q, want override-agent", gotUserAgent)
	}
}

func TestDownloadNon2xxReturnsHTTPStatusAndNoFinalSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		DownloadURL: server.URL,
		DataDir:     dataDir,
		HTTPClient:  server.Client(),
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindHTTPStatus) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindHTTPStatus, err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func TestDownloadContextCanceledReturnsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}`))
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		DownloadURL: server.URL,
		DataDir:     dataDir,
		HTTPClient:  server.Client(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := source.Download(ctx, countryimport.DownloadOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindTimeout) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindTimeout, err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func TestDownloadBadJSONShapeReturnsRemoteDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}]`))
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	store := &recordingMetadataStore{}
	source := NewSource(Config{
		DownloadURL:   server.URL,
		DataDir:       dataDir,
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
	assertNoFinalSnapshots(t, dataDir)
}

func assertNoFinalSnapshots(t *testing.T, dataDir string) {
	t.Helper()

	files, err := os.ReadDir(filepath.Join(dataDir, "snapshots"))
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatalf("read snapshots dir: %v", err)
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".json") {
			t.Fatalf("unexpected final snapshot left behind: %s", file.Name())
		}
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
