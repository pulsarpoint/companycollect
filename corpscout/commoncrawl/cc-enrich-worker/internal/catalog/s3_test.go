package catalog

import (
	"bytes"
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
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const testSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestDeriveCatalogLocation(t *testing.T) {
	location, err := deriveCatalogLocation(
		"s3://crawls/commoncrawl/catalogs/",
		"CC-MAIN-2026-25",
		"pages25",
	)
	if err != nil {
		t.Fatal(err)
	}
	if location.bucket != "crawls" {
		t.Fatalf("bucket = %q", location.bucket)
	}
	if location.directory != "commoncrawl/catalogs/CC-MAIN-2026-25/pages25" {
		t.Fatalf("directory = %q", location.directory)
	}
	if location.readyKey != location.directory+"/ready.json" ||
		location.catalogKey != location.directory+"/catalog.duckdb" {
		t.Fatalf("derived keys = %+v", location)
	}
}

func TestDeriveCatalogLocationRejectsAmbiguousInputs(t *testing.T) {
	for _, test := range []struct {
		name      string
		baseURI   string
		crawlID   string
		selection string
	}{
		{name: "wrong scheme", baseURI: "https://crawls/catalogs", crawlID: "CC-MAIN-2026-25", selection: "pages25"},
		{name: "missing bucket", baseURI: "s3:///catalogs", crawlID: "CC-MAIN-2026-25", selection: "pages25"},
		{name: "query", baseURI: "s3://crawls/catalogs?version=1", crawlID: "CC-MAIN-2026-25", selection: "pages25"},
		{name: "empty prefix component", baseURI: "s3://crawls/catalogs//published", crawlID: "CC-MAIN-2026-25", selection: "pages25"},
		{name: "crawl path", baseURI: "s3://crawls/catalogs", crawlID: "CC-MAIN/2026-25", selection: "pages25"},
		{name: "selection path", baseURI: "s3://crawls/catalogs", crawlID: "CC-MAIN-2026-25", selection: "pages/25"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := deriveCatalogLocation(test.baseURI, test.crawlID, test.selection); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestDecodeReadyManifest(t *testing.T) {
	location, err := deriveCatalogLocation("s3://crawls/catalogs", "CC-MAIN-2026-25", "pages25")
	if err != nil {
		t.Fatal(err)
	}
	valid := readyManifest{
		SchemaVersion: 1,
		CrawlID:       "CC-MAIN-2026-25",
		Selection:     "pages25",
		Catalog:       readyObject{Key: location.catalogKey, SizeBytes: 100, SHA256: testSHA256},
	}

	body := readyManifestJSON(t, valid, true)
	manifest, err := decodeReadyManifest(body, location, valid.CrawlID, valid.Selection)
	if err != nil {
		t.Fatal(err)
	}
	if manifest != valid {
		t.Fatalf("manifest = %+v, want %+v", manifest, valid)
	}

	tests := []struct {
		name      string
		mutate    func(*readyManifest)
		wantError string
	}{
		{name: "schema", mutate: func(manifest *readyManifest) { manifest.SchemaVersion = 2 }, wantError: "schema_version=2"},
		{name: "identity", mutate: func(manifest *readyManifest) { manifest.CrawlID = "CC-MAIN-2025-30" }, wantError: "catalog commit identity"},
		{name: "redirect", mutate: func(manifest *readyManifest) { manifest.Catalog.Key = "other/catalog.duckdb" }, wantError: "catalog key"},
		{name: "empty object", mutate: func(manifest *readyManifest) { manifest.Catalog.SizeBytes = 0 }, wantError: "size_bytes must be positive"},
		{name: "checksum", mutate: func(manifest *readyManifest) { manifest.Catalog.SHA256 = strings.ToUpper(testSHA256) }, wantError: "64 lowercase hexadecimal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			_, err := decodeReadyManifest(
				readyManifestJSON(t, candidate, false),
				location,
				valid.CrawlID,
				valid.Selection,
			)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func readyManifestJSON(t *testing.T, manifest readyManifest, additiveMetadata bool) []byte {
	t.Helper()
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !additiveMetadata {
		return body
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	fields["published_at"] = "2026-07-13T10:00:00Z"
	body, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestValidateCatalogObjectChecksHeadMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodHead || request.URL.Path != "/crawls/catalogs/catalog.duckdb" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		response.Header().Set("Content-Length", "10")
		response.Header().Set("x-amz-meta-sha256", testSHA256)
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := testS3Client(t, server.URL)
	object := readyObject{Key: "catalogs/catalog.duckdb", SizeBytes: 10, SHA256: testSHA256}
	if err := validateCatalogObject(context.Background(), client, "crawls", object); err != nil {
		t.Fatal(err)
	}

	wrongSize := object
	wrongSize.SizeBytes = 11
	if err := validateCatalogObject(context.Background(), client, "crawls", wrongSize); err == nil ||
		!strings.Contains(err.Error(), "size is 10") {
		t.Fatalf("size error = %v", err)
	}

	wrongChecksum := object
	wrongChecksum.SHA256 = strings.Repeat("a", 64)
	if err := validateCatalogObject(context.Background(), client, "crawls", wrongChecksum); err == nil ||
		!strings.Contains(err.Error(), "sha256 metadata") {
		t.Fatalf("checksum error = %v", err)
	}
}

func TestEnsureLocalCatalogDownloadsThenReuses(t *testing.T) {
	state, client, location := newCacheS3Server(t, []byte("first catalog"))
	defer state.server.Close()
	cacheBase := t.TempDir()

	path, err := ensureLocalCatalog(
		context.Background(), client, location, cacheBase, "CC-MAIN-2026-25", "pages25",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertFileBody(t, path, []byte("first catalog"))
	checksum := sha256Hex([]byte("first catalog"))
	assertFileBody(t, path+".sha256", []byte(checksum+"\n"))

	if err := os.WriteFile(path+".partial", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".sha256.partial", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	reused, err := ensureLocalCatalog(
		context.Background(), client, location, cacheBase, "CC-MAIN-2026-25", "pages25",
	)
	if err != nil {
		t.Fatal(err)
	}
	if reused != path || state.catalogGETs() != 1 {
		t.Fatalf("reused path=%q downloads=%d", reused, state.catalogGETs())
	}
	for _, partial := range []string{path + ".partial", path + ".sha256.partial"} {
		if _, err := os.Stat(partial); !os.IsNotExist(err) {
			t.Fatalf("stale partial remains: %s", partial)
		}
	}
}

func TestEnsureLocalCatalogReplacesChangedManifest(t *testing.T) {
	oldCatalog := []byte("catalog-one")
	newCatalog := []byte("catalog-two")
	state, client, location := newCacheS3Server(t, oldCatalog)
	defer state.server.Close()
	cacheBase := t.TempDir()

	path, err := ensureLocalCatalog(
		context.Background(), client, location, cacheBase, "CC-MAIN-2026-25", "pages25",
	)
	if err != nil {
		t.Fatal(err)
	}
	state.setCatalog(newCatalog, bytes.Repeat([]byte{'x'}, len(newCatalog)))
	if _, err := ensureLocalCatalog(
		context.Background(), client, location, cacheBase, "CC-MAIN-2026-25", "pages25",
	); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("changed corrupt download error = %v", err)
	}
	assertFileBody(t, path, oldCatalog)
	assertFileBody(t, path+".sha256", []byte(sha256Hex(oldCatalog)+"\n"))

	state.setCatalog(newCatalog, newCatalog)
	replaced, err := ensureLocalCatalog(
		context.Background(), client, location, cacheBase, "CC-MAIN-2026-25", "pages25",
	)
	if err != nil {
		t.Fatal(err)
	}
	if replaced != path {
		t.Fatalf("replaced path = %q, want %q", replaced, path)
	}
	assertFileBody(t, path, newCatalog)
	assertFileBody(t, path+".sha256", []byte(sha256Hex(newCatalog)+"\n"))
}

func TestEnsureLocalCatalogRejectsCorruptDownloadAndCleansPartial(t *testing.T) {
	goodCatalog := []byte("correct catalog")
	corruptCatalog := bytes.Repeat([]byte{'x'}, len(goodCatalog))
	state, client, location := newCacheS3Server(t, goodCatalog)
	defer state.server.Close()
	state.setCatalog(goodCatalog, corruptCatalog)
	cacheBase := t.TempDir()

	_, err := ensureLocalCatalog(
		context.Background(), client, location, cacheBase, "CC-MAIN-2026-25", "pages25",
	)
	if err == nil || !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("error = %v", err)
	}
	if state.catalogGETs() != catalogDownloadTries {
		t.Fatalf("downloads = %d, want %d", state.catalogGETs(), catalogDownloadTries)
	}
	directory := filepath.Join(cacheBase, "CC-MAIN-2026-25", "warc-index", "pages25")
	for _, name := range []string{
		"catalog.duckdb",
		"catalog.duckdb.sha256",
		"catalog.duckdb.partial",
		"catalog.duckdb.sha256.partial",
	} {
		if _, err := os.Stat(filepath.Join(directory, name)); !os.IsNotExist(err) {
			t.Fatalf("failed download left %s", name)
		}
	}
}

type cacheS3Server struct {
	t        *testing.T
	server   *httptest.Server
	location catalogLocation

	mutex       sync.Mutex
	expected    []byte
	download    []byte
	downloadGET int
}

func newCacheS3Server(
	t *testing.T,
	catalog []byte,
) (*cacheS3Server, *s3.Client, catalogLocation) {
	t.Helper()
	location, err := deriveCatalogLocation("s3://crawls/catalogs", "CC-MAIN-2026-25", "pages25")
	if err != nil {
		t.Fatal(err)
	}
	state := &cacheS3Server{
		t:        t,
		location: location,
		expected: append([]byte(nil), catalog...),
		download: append([]byte(nil), catalog...),
	}
	state.server = httptest.NewServer(state)
	return state, testS3Client(t, state.server.URL), location
}

func (state *cacheS3Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	state.mutex.Lock()
	expected := append([]byte(nil), state.expected...)
	download := append([]byte(nil), state.download...)
	state.mutex.Unlock()

	readyPath := "/crawls/" + state.location.readyKey
	catalogPath := "/crawls/" + state.location.catalogKey
	switch {
	case request.Method == http.MethodGet && request.URL.Path == readyPath:
		manifest := readyManifest{
			SchemaVersion: readySchemaVersion,
			CrawlID:       "CC-MAIN-2026-25",
			Selection:     "pages25",
			Catalog: readyObject{
				Key:       state.location.catalogKey,
				SizeBytes: int64(len(expected)),
				SHA256:    sha256Hex(expected),
			},
		}
		body, err := json.Marshal(manifest)
		if err != nil {
			state.t.Errorf("marshal manifest: %v", err)
			response.WriteHeader(http.StatusInternalServerError)
			return
		}
		response.Header().Set("Content-Length", strconv.Itoa(len(body)))
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write(body)
	case request.Method == http.MethodHead && request.URL.Path == catalogPath:
		response.Header().Set("Content-Length", strconv.Itoa(len(expected)))
		response.Header().Set("x-amz-meta-sha256", sha256Hex(expected))
		response.WriteHeader(http.StatusOK)
	case request.Method == http.MethodGet && request.URL.Path == catalogPath:
		state.mutex.Lock()
		state.downloadGET++
		state.mutex.Unlock()
		response.Header().Set("Content-Length", strconv.Itoa(len(download)))
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write(download)
	default:
		state.t.Errorf("unexpected S3 request: %s %s", request.Method, request.URL.Path)
		response.WriteHeader(http.StatusNotFound)
	}
}

func (state *cacheS3Server) setCatalog(expected, download []byte) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	state.expected = append([]byte(nil), expected...)
	state.download = append([]byte(nil), download...)
}

func (state *cacheS3Server) catalogGETs() int {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	return state.downloadGET
}

func sha256Hex(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func assertFileBody(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func testS3Client(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	client, err := newS3Client(context.Background(), S3Config{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		AccessKey: "test-access",
		SecretKey: "test-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
