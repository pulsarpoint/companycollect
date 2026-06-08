package coloradoentities

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
	"strconv"
	"strings"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

var sampleEntities = []string{
	`{"entityid":"20251665680","entityname":"KYLDERON MIST VALLEY LLC","entitystatus":"Good Standing","jurisdictonofformation":"CO","entitytype":"DLLC"}`,
	`{"entityid":"19871342214","entityname":"SOUTHWEST CONTRACTING, LLC","entitystatus":"Delinquent","jurisdictonofformation":"CO","entitytype":"DLLC"}`,
	`{"entityid":"20251955891","entityname":"FIRST HEALTH LIFE & HEALTH INSURANCE COMPANY","entitystatus":"Good Standing","jurisdictonofformation":"TX","entitytype":"FPC"}`,
}

// pagedHandler serves the sampleEntities window for a $limit/$offset query.
func pagedHandler(t *testing.T, gotToken *string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if gotToken != nil {
			*gotToken = r.Header.Get("X-App-Token")
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("$limit"))
		offset, _ := strconv.Atoi(r.URL.Query().Get("$offset"))
		if limit <= 0 {
			limit = len(sampleEntities)
		}
		window := make([]json.RawMessage, 0, limit)
		for i := offset; i < offset+limit && i < len(sampleEntities); i++ {
			window = append(window, json.RawMessage(sampleEntities[i]))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(window)
	}
}

func TestDownloadPagesUntilShortPage(t *testing.T) {
	var gotToken string
	server := httptest.NewServer(pagedHandler(t, &gotToken))
	t.Cleanup(server.Close)

	store := &recordingMetadataStore{}
	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:        server.URL,
		DataDir:        dataDir,
		PageSize:       2,
		AppToken:       "secret-token",
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		PageDelay:      time.Millisecond,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if result.RecordsSeen != 3 {
		t.Fatalf("RecordsSeen = %d, want 3", result.RecordsSeen)
	}
	// pageSize 2 over 3 records -> page1 (2) + page2 (1, short) = 2 pages.
	if result.PagesDownloaded != 2 {
		t.Fatalf("PagesDownloaded = %d, want 2", result.PagesDownloaded)
	}
	if !strings.HasPrefix(filepath.Base(result.SnapshotPath), "colorado_entities_") ||
		!strings.HasSuffix(result.SnapshotPath, ".ndjson") {
		t.Fatalf("SnapshotPath = %q", result.SnapshotPath)
	}
	if gotToken != "secret-token" {
		t.Fatalf("X-App-Token = %q, want secret-token", gotToken)
	}

	lines := readSnapshotLines(t, result.SnapshotPath)
	if len(lines) != 3 {
		t.Fatalf("snapshot line count = %d, want 3", len(lines))
	}
	var first ColoradoEntityRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first line: %v", err)
	}
	if first.EntityID != "20251665680" || first.JurisdictionOfFormation != "CO" {
		t.Fatalf("first record = %#v (misspelled jurisdiction key must map)", first)
	}

	contents, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	sum := sha256.Sum256(contents)
	if result.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("SHA256 mismatch")
	}
	if store.downloadCalls != 1 {
		t.Fatalf("SaveDownload calls = %d, want 1", store.downloadCalls)
	}
	if store.download.Attribution == "" {
		t.Fatal("metadata Attribution is empty")
	}
}

func TestDownloadMaxPagesLimitsPages(t *testing.T) {
	server := httptest.NewServer(pagedHandler(t, nil))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    t.TempDir(),
		PageSize:   1,
		HTTPClient: server.Client(),
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{MaxPages: 2})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if result.PagesDownloaded != 2 || result.RecordsSeen != 2 {
		t.Fatalf("pages=%d records=%d, want 2/2", result.PagesDownloaded, result.RecordsSeen)
	}
}

func TestDownloadNon2xxReturnsHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    dataDir,
		HTTPClient: server.Client(),
	})

	// Single retry budget is internal; non-retryable 429 still classifies as http_status after retries.
	_, err := source.Download(context.Background(), countryimport.DownloadOptions{MaxPages: 1})
	if !countryimport.IsKind(err, countryimport.ErrorKindHTTPStatus) {
		t.Fatalf("Download error kind = %v, want http_status; err=%v", countryimport.Classify(err), err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func TestDownloadBadJSONReturnsRemoteDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"not":"an array"}`))
	}))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    dataDir,
		HTTPClient: server.Client(),
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{MaxPages: 1})
	if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
		t.Fatalf("Download error kind = %v, want remote_decode; err=%v", countryimport.Classify(err), err)
	}
	assertNoFinalSnapshots(t, dataDir)
}

func TestDownloadContextCanceledReturnsTimeout(t *testing.T) {
	server := httptest.NewServer(pagedHandler(t, nil))
	t.Cleanup(server.Close)

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL:    server.URL,
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
