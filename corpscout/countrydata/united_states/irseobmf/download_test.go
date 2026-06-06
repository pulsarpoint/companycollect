package irseobmf

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

func TestDownloadConvertsCSVToNDJSONAndSkipsMalformedRows(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "testdata/eo_sample.csv")
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURLs:   []string{server.URL + "/eo1.csv"},
		DataDir:        t.TempDir(),
		HTTPClient:     server.Client(),
		MetadataStore:  store,
		RequestTimeout: time.Second,
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	// 4 well-formed rows; the 3-field "BROKEN ROW INC" is skipped.
	if result.RecordsSeen != 4 {
		t.Fatalf("RecordsSeen = %d, want 4", result.RecordsSeen)
	}
	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1", result.PagesDownloaded)
	}

	data, err := os.ReadFile(result.SnapshotPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("snapshot line count = %d, want 4", len(lines))
	}

	// Each line must be a JSON object with the canonical column keys.
	var first IrsEoBmfRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first line: %v", err)
	}
	if first.EIN != "010011694" || first.Name != "MASSACHUSETTS MODERATORS ASSOCIATION INC" {
		t.Fatalf("first record = %#v, want Massachusetts Moderators", first)
	}
	if first.NteeCd != "S19" || first.AssetAmt != "65979" {
		t.Fatalf("first record fields not preserved: %#v", first)
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
	if store.download.SHA256 != result.SHA256 {
		t.Fatalf("metadata SHA256 = %q, want %q", store.download.SHA256, result.SHA256)
	}
}

func TestDownloadAggregatesMultipleFiles(t *testing.T) {
	header := strings.Join(csvColumns, ",")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		switch r.URL.Path {
		case "/eo1.csv":
			_, _ = w.Write([]byte(header + "\n010011694,ORG ONE,,1 A ST,BOSTON,MA,02101,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,1,2,3,S19,\n"))
		case "/eo2.csv":
			_, _ = w.Write([]byte(header + "\n200011695,ORG TWO,,2 B ST,DENVER,CO,80201,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,4,5,6,B20,\n300011696,ORG THREE,,3 C ST,MIAMI,FL,33101,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,7,8,9,C30,\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURLs: []string{server.URL + "/eo1.csv", server.URL + "/eo2.csv"},
		DataDir:      t.TempDir(),
		HTTPClient:   server.Client(),
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{PageDelay: time.Nanosecond})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	if result.PagesDownloaded != 2 {
		t.Fatalf("PagesDownloaded = %d, want 2", result.PagesDownloaded)
	}
	if result.RecordsSeen != 3 {
		t.Fatalf("RecordsSeen = %d, want 3 (1 from eo1 + 2 from eo2)", result.RecordsSeen)
	}
}

func TestDownloadMaxPagesLimitsFiles(t *testing.T) {
	header := strings.Join(csvColumns, ",")
	row := func(ein string) string {
		return ein + ",ORG,,1 A ST,BOSTON,MA,02101,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,1,2,3,S19,"
	}
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		_, _ = w.Write([]byte(header + "\n" + row("010011694") + "\n"))
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURLs: []string{server.URL + "/eo1.csv", server.URL + "/eo2.csv", server.URL + "/eo3.csv"},
		DataDir:      t.TempDir(),
		HTTPClient:   server.Client(),
	})

	result, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:  1,
		PageDelay: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if result.PagesDownloaded != 1 {
		t.Fatalf("PagesDownloaded = %d, want 1", result.PagesDownloaded)
	}
	if len(requested) != 1 {
		t.Fatalf("requested files = %v, want only the first", requested)
	}
}

func TestDownloadReturnsHTTPStatusKindOnError(t *testing.T) {
	store := &recordingMetadataStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	source := NewSource(Config{
		DownloadURLs:  []string{server.URL + "/eo1.csv"},
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
