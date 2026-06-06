package coloradoentities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestDownloadWritesNDJSONSnapshotAndMetadata(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First page returns the fixture; any later offset returns an empty array.
		if r.URL.Query().Get("$offset") == "0" {
			http.ServeFile(w, r, "testdata/co_entities.json")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:        server.URL,
		DataDir:        t.TempDir(),
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	if result.RecordsSeen != 4 {
		t.Fatalf("RecordsSeen = %d, want 4", result.RecordsSeen)
	}
	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1 (fixture < page size)", result.PagesDownloaded)
	}

	data, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("snapshot line count = %d, want 4", len(lines))
	}

	var first ColoradoEntityRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first line: %v", err)
	}
	if first.EntityID != "20251665680" {
		t.Fatalf("first record EntityID = %q, want 20251665680", first.EntityID)
	}
	// The misspelled jurisdiction key must survive the round trip.
	if first.JurisdictonOfFormation != "CO" {
		t.Fatalf("first record JurisdictonOfFormation = %q, want CO", first.JurisdictonOfFormation)
	}

	sum := sha256.Sum256(data)
	if result.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %q, want %q", result.SHA256, hex.EncodeToString(sum[:]))
	}
	if store.downloadCalls != 1 {
		t.Fatalf("SaveDownload calls = %d, want 1", store.downloadCalls)
	}
	if store.download.RecordsSeen != result.RecordsSeen {
		t.Fatalf("metadata RecordsSeen = %d, want %d", store.download.RecordsSeen, result.RecordsSeen)
	}
}

func TestDownloadPaginatesWithLimitOffset(t *testing.T) {
	var seenOffsets []string
	var seenLimits []string
	var seenOrders []string
	entity := func(id string) string {
		return `{"entityid":"` + id + `","entityname":"ORG ` + id + `","entitystatus":"Good Standing"}`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenOffsets = append(seenOffsets, r.URL.Query().Get("$offset"))
		seenLimits = append(seenLimits, r.URL.Query().Get("$limit"))
		seenOrders = append(seenOrders, r.URL.Query().Get("$order"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$offset") {
		case "0":
			_, _ = w.Write([]byte("[" + entity("1") + "," + entity("2") + "]"))
		case "2":
			_, _ = w.Write([]byte("[" + entity("3") + "]"))
		default:
			_, _ = w.Write([]byte("[]"))
		}
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
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
	if result.PagesDownloaded != 2 {
		t.Fatalf("PagesDownloaded = %d, want 2 (short second page stops paging)", result.PagesDownloaded)
	}
	if len(seenOffsets) != 2 || seenOffsets[0] != "0" || seenOffsets[1] != "2" {
		t.Fatalf("offsets = %v, want [0 2]", seenOffsets)
	}
	if seenLimits[0] != "2" {
		t.Fatalf("limit = %q, want 2", seenLimits[0])
	}
	if seenOrders[0] != "entityid" {
		t.Fatalf("order = %q, want entityid", seenOrders[0])
	}
}

func TestDownloadSendsAppTokenHeaderWhenConfigured(t *testing.T) {
	var gotToken string
	var tokenSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-App-Token")
		_, tokenSeen = r.Header["X-App-Token"]
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    t.TempDir(),
		AppToken:   "secret-token",
		HTTPClient: server.Client(),
	})

	if _, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond}); err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if !tokenSeen || gotToken != "secret-token" {
		t.Fatalf("X-App-Token header = %q (seen=%v), want secret-token", gotToken, tokenSeen)
	}
}

func TestDownloadOmitsAppTokenHeaderWhenUnset(t *testing.T) {
	var tokenSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, tokenSeen = r.Header["X-App-Token"]
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    t.TempDir(),
		HTTPClient: server.Client(),
	})

	if _, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond}); err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if tokenSeen {
		t.Fatal("X-App-Token header present, want absent when no token configured")
	}
}

func TestDownloadReturnsRemoteDecodeForNonArrayBody(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":"not an array"}`))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:       server.URL,
		DataDir:       t.TempDir(),
		HTTPClient:    server.Client(),
		MetadataStore: store,
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if !countryimport.IsKind(err, countryimport.ErrorKindRemoteDecode) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindRemoteDecode, err)
	}
	if store.downloadCalls != 0 {
		t.Fatalf("SaveDownload calls = %d, want 0", store.downloadCalls)
	}
}

func TestDownloadReturnsHTTPStatusKindOnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    t.TempDir(),
		HTTPClient: server.Client(),
	})

	_, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if !countryimport.IsKind(err, countryimport.ErrorKindHTTPStatus) {
		t.Fatalf("Download error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindHTTPStatus, err)
	}
}

func TestDownloadMaxPagesLimitsRequests(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		offset, _ := strconv.Atoi(r.URL.Query().Get("$offset"))
		w.Header().Set("Content-Type", "application/json")
		// Always return a full page so paging would continue without the cap.
		_, _ = w.Write([]byte(`[{"entityid":"` + strconv.Itoa(offset) + `","entityname":"ORG"}]`))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		BaseURL:    server.URL,
		DataDir:    t.TempDir(),
		PageSize:   1,
		HTTPClient: server.Client(),
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:  2,
		PageDelay: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if result.PagesDownloaded != 2 {
		t.Fatalf("PagesDownloaded = %d, want 2", result.PagesDownloaded)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
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
