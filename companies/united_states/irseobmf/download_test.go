package irseobmf

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func readCSVFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "eo_bmf_sample.csv"))
	if err != nil {
		t.Fatalf("read CSV fixture: %v", err)
	}
	return data
}

func TestDownloadWritesSnapshotAndMetadata(t *testing.T) {
	csv := readCSVFixture(t)
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(csv)
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:        server.URL + "/",
		Files:          []string{"eo1.csv", "eo2.csv"},
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
	if result.PagesDownloaded != 2 {
		t.Fatalf("PagesDownloaded = %d, want 2", result.PagesDownloaded)
	}
	// 3 valid rows per file (the 4th fixture row is malformed and skipped).
	if result.RecordsSeen != 6 {
		t.Fatalf("RecordsSeen = %d, want 6", result.RecordsSeen)
	}
	if !strings.HasPrefix(filepath.Base(result.SnapshotPath), "irs_eo_bmf_") ||
		!strings.HasSuffix(result.SnapshotPath, ".ndjson") {
		t.Fatalf("SnapshotPath = %q, want irs_eo_bmf_*.ndjson", result.SnapshotPath)
	}

	lines := readSnapshotLines(t, result.SnapshotPath)
	if len(lines) != 6 {
		t.Fatalf("snapshot line count = %d, want 6", len(lines))
	}
	var first IrsEoBmfRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first snapshot line: %v", err)
	}
	if first.EIN != "010011694" || first.Name != "MASSACHUSETTS MODERATORS ASSOCIATION INC" {
		t.Fatalf("first record = %#v", first)
	}
	// Second record EIN is 8 digits in the source and must be normalized.
	var second IrsEoBmfRecord
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("decode second snapshot line: %v", err)
	}
	if second.EIN != "010018605" {
		t.Fatalf("second record EIN = %q, want normalized 010018605", second.EIN)
	}

	contents, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	sum := sha256.Sum256(contents)
	if result.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 mismatch")
	}

	if store.downloadCalls != 1 {
		t.Fatalf("SaveDownload calls = %d, want 1", store.downloadCalls)
	}
	if store.download.HTTPStatuses["eo1.csv"] != http.StatusOK {
		t.Fatalf("metadata HTTPStatuses[eo1.csv] = %d, want 200", store.download.HTTPStatuses["eo1.csv"])
	}
	if store.download.Attribution == "" {
		t.Fatal("metadata Attribution is empty")
	}
}

func TestDownloadMaxPagesLimitsFiles(t *testing.T) {
	csv := readCSVFixture(t)
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, strings.TrimPrefix(r.URL.Path, "/"))
		_, _ = w.Write(csv)
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL + "/",
		Files:      []string{"eo1.csv", "eo2.csv", "eo3.csv", "eo4.csv"},
		DataDir:    t.TempDir(),
		HTTPClient: server.Client(),
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{MaxPages: 1})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1", result.PagesDownloaded)
	}
	if len(requested) != 1 || requested[0] != "eo1.csv" {
		t.Fatalf("requested files = %#v, want [eo1.csv]", requested)
	}
}

func TestDownloadInvalidHeaderReturnsRemoteDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("EIN,NAME,WRONG\n010011694,FOO,BAR\n"))
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:    server.URL + "/",
		Files:      []string{"eo1.csv"},
		DataDir:    dataDir,
		HTTPClient: server.Client(),
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
		t.Fatalf("Download error kind = %v, want remote_decode; err=%v", countryimport.Classify(err), err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func TestDownloadNon2xxReturnsHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:    server.URL + "/",
		Files:      []string{"eo1.csv"},
		DataDir:    dataDir,
		HTTPClient: server.Client(),
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindHTTPStatus) {
		t.Fatalf("Download error kind = %v, want http_status; err=%v", countryimport.Classify(err), err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func TestDownloadContextCanceledReturnsTimeout(t *testing.T) {
	csv := readCSVFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(csv)
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:    server.URL + "/",
		Files:      []string{"eo1.csv"},
		DataDir:    dataDir,
		HTTPClient: server.Client(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := source.Download(ctx, countryimport.DownloadOptions{})
	if !countryimport.IsKind(err, countryimport.ErrorKindTimeout) {
		t.Fatalf("Download error kind = %v, want timeout; err=%v", countryimport.Classify(err), err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func readSnapshotLines(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSnapshotLineBytes)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			lines = append(lines, scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan snapshot: %v", err)
	}
	return lines
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
		if strings.HasSuffix(file.Name(), ".ndjson") {
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
