package rawdownload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cc-raw/rawstate"
	"cc-raw/rawstore"
	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

func TestDownloaderCommitsChunksThenReadyAndResumes(t *testing.T) {
	ctx := context.Background()
	objectServer := newS3TestServer(t, "crawls")
	store := newTestStore(t, ctx, objectServer.URL(), "crawls")
	source := newTestSource()
	worklistPath := filepath.Join(t.TempDir(), "shard_tech_0.parquet")
	rows := []worklistRow{
		testWorklistRow(source, "example.com", "https://example.com/", "warc-0.gz", 10, []byte("record-one"), stringPointer("eng")),
		testWorklistRow(source, "example.com", "https://example.com/contact", "warc-0.gz", 100, []byte("record-two"), stringPointer("eng")),
		testWorklistRow(source, "example.org", "https://example.org/", "warc-1.gz", 20, []byte("record-three"), nil),
	}
	if err := parquet.WriteFile(worklistPath, rows); err != nil {
		t.Fatal(err)
	}

	downloader := Downloader{
		Source: source,
		Store:  store,
		Logger: testLogger(),
		Config: testDownloaderConfig(worklistPath),
	}
	result, err := downloader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped || result.ChunkCount != 2 || result.DownloadedRecords != 3 || result.FailedRecords != 0 {
		t.Fatalf("unexpected result %+v", result)
	}

	expectedOrder := []string{
		"commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/records.pack",
		"commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/index.parquet",
		"commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/manifest.json",
		"commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000001/records.pack",
		"commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000001/index.parquet",
		"commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000001/manifest.json",
		"commoncrawl/state/crawl=CC-MAIN-2026-25/selection=pages25/part=000/download/ready.json",
	}
	if got := objectServer.putOrder(); !equalStrings(got, expectedOrder) {
		t.Fatalf("put order\ngot:  %v\nwant: %v", got, expectedOrder)
	}
	pack := objectServer.body(expectedOrder[0])
	if want := []byte("record-onerecord-two"); !bytes.Equal(pack, want) {
		t.Fatalf("pack body=%q, want %q", pack, want)
	}
	indexBody := objectServer.body(expectedOrder[1])
	indexRows, err := parquet.Read[rawstore.IndexRow](bytes.NewReader(indexBody), int64(len(indexBody)))
	if err != nil {
		t.Fatal(err)
	}
	if len(indexRows) != 2 || !indexRows[0].IsPrimary || indexRows[1].IsPrimary || indexRows[1].DomainRank != 0 {
		t.Fatalf("unexpected chunk index rows %+v", indexRows)
	}
	ready, err := rawstore.DecodeReadyManifest(objectServer.body(expectedOrder[len(expectedOrder)-1]))
	if err != nil {
		t.Fatal(err)
	}
	if ready.Totals.ChunkCount != 2 || ready.Totals.DownloadedRecords != 3 {
		t.Fatalf("unexpected ready manifest %+v", ready.Totals)
	}

	sourceCalls := source.callCount()
	putCount := len(objectServer.putOrder())
	result, err = downloader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Skipped {
		t.Fatal("ready part was not skipped")
	}
	if source.callCount() != sourceCalls || len(objectServer.putOrder()) != putCount {
		t.Fatal("resume performed new downloads or uploads")
	}

	objectServer.setChecksum(expectedOrder[0], "corrupt")
	sourceCalls = source.callCount()
	putCount = len(objectServer.putOrder())
	result, err = downloader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped {
		t.Fatal("part with corrupt pack metadata was skipped")
	}
	if source.callCount() != sourceCalls+2 {
		t.Fatalf("corrupt first chunk caused %d source calls, want 2", source.callCount()-sourceCalls)
	}
	if got := objectServer.putOrder()[putCount:]; !equalStrings(got, []string{expectedOrder[0], expectedOrder[1], expectedOrder[2], expectedOrder[6]}) {
		t.Fatalf("unexpected repair uploads %v", got)
	}
}

func TestPlanChunksHonorsByteLimitAndKeepsOversizedRecordWhole(t *testing.T) {
	records := []selectedRecord{
		{worklistRow: worklistRow{WARCRecordLength: 6}},
		{worklistRow: worklistRow{WARCRecordLength: 6}},
		{worklistRow: worklistRow{WARCRecordLength: 20}},
		{worklistRow: worklistRow{WARCRecordLength: 4}},
	}
	chunks := planChunks(records, 10, 100)
	if len(chunks) != 4 {
		t.Fatalf("chunk count=%d, want 4", len(chunks))
	}
	for index, chunk := range chunks {
		if chunk.Number != index || len(chunk.Records) != 1 {
			t.Fatalf("chunk %d=%+v", index, chunk)
		}
	}
	if chunks[2].Records[0].WARCRecordLength != 20 {
		t.Fatal("oversized record was split or misplaced")
	}
}

func TestDownloadFailureReasons(t *testing.T) {
	tests := []struct {
		err    error
		status rawstore.DownloadStatus
		code   string
	}{
		{err: context.DeadlineExceeded, status: rawstore.Failed, code: "timeout"},
		{err: errors.New("http 404"), status: rawstore.NotFound, code: "not_found"},
		{err: errors.New("http 429"), status: rawstore.Failed, code: "throttled"},
		{err: errors.New("access denied"), status: rawstore.Failed, code: "access_denied"},
		{err: errors.New("short WARC range"), status: rawstore.Failed, code: "short_read"},
		{err: errors.New("connection reset by peer"), status: rawstore.Failed, code: "connection_reset"},
	}
	for _, test := range tests {
		status, code := classifyDownloadError(test.err)
		if status != test.status || code != test.code {
			t.Errorf("classifyDownloadError(%q)=(%q, %q), want (%q, %q)", test.err, status, code, test.status, test.code)
		}
	}

	results := summarizeDownloads([]recordDownload{
		{status: rawstore.Downloaded, attempts: 1},
		{status: rawstore.NotFound, errorCode: "not_found", attempts: 1},
		{status: rawstore.Failed, errorCode: "timeout", attempts: 3},
		{status: rawstore.Failed, errorCode: "connection_reset", attempts: 3},
	})
	if results.RequestedRecords != 4 || results.DownloadedRecords != 1 || results.FailedRecords != 3 {
		t.Fatalf("unexpected summary %+v", results)
	}
	if results.Errors.NotFound != 1 || results.Errors.Timeout != 1 || results.Errors.Other != 1 {
		t.Fatalf("unexpected error classes %+v", results.Errors)
	}
	if results.FailureReasons["not_found"] != 1 || results.FailureReasons["timeout"] != 1 || results.FailureReasons["connection_reset"] != 1 {
		t.Fatalf("unexpected failure reasons %+v", results.FailureReasons)
	}
}

func TestDownloaderRetriesTransientRecordFailures(t *testing.T) {
	ctx := context.Background()
	objectServer := newS3TestServer(t, "crawls")
	store := newTestStore(t, ctx, objectServer.URL(), "crawls")
	source := newTestSource()
	worklistPath := filepath.Join(t.TempDir(), "part_000.parquet")
	row := testWorklistRow(source, "example.com", "https://example.com/", "warc.gz", 10, []byte("record-one"), nil)
	source.failNext("warc.gz", 10, 2)
	if err := parquet.WriteFile(worklistPath, []worklistRow{row}); err != nil {
		t.Fatal(err)
	}
	config := testDownloaderConfig(worklistPath)
	config.MaxRecords = 1
	config.RecordAttempts = 3
	downloader := Downloader{Source: source, Store: store, Logger: testLogger(), Config: config}

	result, err := downloader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.DownloadedRecords != 1 || result.FailedRecords != 0 || source.callCount() != 3 {
		t.Fatalf("unexpected retry result=%+v source_calls=%d", result, source.callCount())
	}
	indexKey := "commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/index.parquet"
	indexBody := objectServer.body(indexKey)
	indexRows, err := parquet.Read[rawstore.IndexRow](bytes.NewReader(indexBody), int64(len(indexBody)))
	if err != nil {
		t.Fatal(err)
	}
	if len(indexRows) != 1 || indexRows[0].DownloadAttempts != 3 || indexRows[0].DownloadStatus != rawstore.Downloaded {
		t.Fatalf("unexpected index rows %+v", indexRows)
	}
}

func TestDownloaderCommitsDescriptiveTerminalFailures(t *testing.T) {
	ctx := context.Background()
	objectServer := newS3TestServer(t, "crawls")
	store := newTestStore(t, ctx, objectServer.URL(), "crawls")
	source := newTestSource()
	worklistPath := filepath.Join(t.TempDir(), "part_000.parquet")
	rows := []worklistRow{
		testWorklistRow(source, "example.com", "https://example.com/", "warc.gz", 0, []byte("record-one"), nil),
		{
			RootDomain:       "missing.example",
			URL:              "https://missing.example/",
			WARCFilename:     "missing.warc.gz",
			WARCRecordOffset: 100,
			WARCRecordLength: 10,
		},
	}
	if err := parquet.WriteFile(worklistPath, rows); err != nil {
		t.Fatal(err)
	}
	config := testDownloaderConfig(worklistPath)
	config.MaxRecords = 2
	config.MaxFailureRate = 0.5
	config.RecordAttempts = 3
	downloader := Downloader{Source: source, Store: store, Logger: testLogger(), Config: config}

	result, err := downloader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.DownloadedRecords != 1 || result.FailedRecords != 1 || source.callCount() != 2 {
		t.Fatalf("unexpected result=%+v source_calls=%d", result, source.callCount())
	}
	manifestKey := "commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/manifest.json"
	manifest, err := rawstore.DecodeChunkManifest(objectServer.body(manifestKey))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Results.Errors.NotFound != 1 || manifest.Results.FailureReasons["not_found"] != 1 {
		t.Fatalf("unexpected failure details %+v", manifest.Results)
	}
	indexKey := "commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/index.parquet"
	indexBody := objectServer.body(indexKey)
	indexRows, err := parquet.Read[rawstore.IndexRow](bytes.NewReader(indexBody), int64(len(indexBody)))
	if err != nil {
		t.Fatal(err)
	}
	if len(indexRows) != 2 || indexRows[1].DownloadAttempts != 1 || indexRows[1].ErrorCode == nil || *indexRows[1].ErrorCode != "not_found" {
		t.Fatalf("unexpected failed index row %+v", indexRows)
	}
}

func TestDownloaderDoesNotCommitChunkAboveFailureLimit(t *testing.T) {
	ctx := context.Background()
	objectServer := newS3TestServer(t, "crawls")
	store := newTestStore(t, ctx, objectServer.URL(), "crawls")
	source := newTestSource()
	worklistPath := filepath.Join(t.TempDir(), "shard_tech_0.parquet")
	rows := []worklistRow{
		testWorklistRow(source, "example.com", "https://example.com/", "warc.gz", 0, []byte("record-one"), nil),
		{
			RootDomain:       "example.org",
			URL:              "https://example.org/",
			WARCFilename:     "missing.warc.gz",
			WARCRecordOffset: 100,
			WARCRecordLength: 10,
		},
	}
	if err := parquet.WriteFile(worklistPath, rows); err != nil {
		t.Fatal(err)
	}
	config := testDownloaderConfig(worklistPath)
	config.MaxRecords = 2
	config.MaxFailureRate = 0
	config.RecordAttempts = 3
	downloader := Downloader{Source: source, Store: store, Logger: testLogger(), Config: config}

	if _, err := downloader.Run(ctx); err == nil || !strings.Contains(err.Error(), "failed records") {
		t.Fatalf("expected failure-rate error, got %v", err)
	}
	if got := objectServer.putOrder(); len(got) != 0 {
		t.Fatalf("failed chunk wrote objects: %v", got)
	}
	if source.callCount() != 2 {
		t.Fatalf("not-found record was retried: source calls=%d, want 2", source.callCount())
	}
}

func TestDownloaderRequiresForceAfterReclamation(t *testing.T) {
	ctx := context.Background()
	objectServer := newS3TestServer(t, "crawls")
	store := newTestStore(t, ctx, objectServer.URL(), "crawls")
	source := newTestSource()
	worklistPath := filepath.Join(t.TempDir(), "shard_tech_0.parquet")
	rows := []worklistRow{
		testWorklistRow(source, "example.com", "https://example.com/", "warc.gz", 0, []byte("record-one"), nil),
	}
	if err := parquet.WriteFile(worklistPath, rows); err != nil {
		t.Fatal(err)
	}
	reclaimedKey, err := rawstate.ReclaimedKey("CC-MAIN-2026-25", "pages25", 0)
	if err != nil {
		t.Fatal(err)
	}
	reclaimedBody := []byte("reclaimed")
	if err := store.PutBytes(ctx, reclaimedKey, "application/json", reclaimedBody, rawstore.ChecksumBytes(reclaimedBody)); err != nil {
		t.Fatal(err)
	}
	downloader := Downloader{Source: source, Store: store, Logger: testLogger(), Config: testDownloaderConfig(worklistPath)}
	if _, err := downloader.Run(ctx); err == nil || !strings.Contains(err.Error(), "force-redownload") {
		t.Fatalf("expected reclaimed-state error, got %v", err)
	}
	if source.callCount() != 0 {
		t.Fatal("reclaimed part downloaded without force")
	}

	downloader.Config.ForceRedownload = true
	if _, err := downloader.Run(ctx); err != nil {
		t.Fatal(err)
	}
	exists, err := store.Exists(ctx, reclaimedKey)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("reclaimed marker remained after forced redownload committed ready state")
	}
}

func testDownloaderConfig(worklistPath string) Config {
	return Config{
		WorklistPath:    worklistPath,
		WorklistKey:     filepath.Base(worklistPath),
		CrawlID:         "CC-MAIN-2026-25",
		Selection:       "pages25",
		Part:            0,
		SourceBucket:    "commoncrawl",
		Concurrency:     4,
		MaxPackBytes:    1 << 20,
		MaxRecords:      2,
		MaxFailureRate:  0,
		RecordAttempts:  1,
		RecordTimeout:   time.Second,
		RunID:           "download-test-run",
		WorkerHost:      "test-worker",
		GitCommit:       "test-commit",
		ForceRedownload: false,
	}
}

func testWorklistRow(source *testSource, domain, pageURL, filename string, offset int64, body []byte, languages *string) worklistRow {
	source.add(filename, offset, body)
	return worklistRow{
		RootDomain:       domain,
		URL:              pageURL,
		WARCFilename:     filename,
		WARCRecordOffset: offset,
		WARCRecordLength: int64(len(body)),
		ContentLanguages: languages,
	}
}

type testSource struct {
	mu                sync.Mutex
	records           map[string][]byte
	transientFailures map[string]int
	calls             int
}

func newTestSource() *testSource {
	return &testSource{
		records:           make(map[string][]byte),
		transientFailures: make(map[string]int),
	}
}

func (source *testSource) add(key string, offset int64, body []byte) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.records[fmt.Sprintf("%s:%d", key, offset)] = body
}

func (source *testSource) failNext(key string, offset int64, attempts int) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.transientFailures[fmt.Sprintf("%s:%d", key, offset)] = attempts
}

func (source *testSource) GetRange(_ context.Context, _, key string, start, end int64) ([]byte, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls++
	recordKey := fmt.Sprintf("%s:%d", key, start)
	body, exists := source.records[recordKey]
	if !exists {
		return nil, errors.New("http 404")
	}
	if source.transientFailures[recordKey] > 0 {
		source.transientFailures[recordKey]--
		return nil, errors.New("temporary range failure")
	}
	if end-start+1 != int64(len(body)) {
		return nil, errors.New("unexpected range")
	}
	return append([]byte(nil), body...), nil
}

func (source *testSource) callCount() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.calls
}

type storedObject struct {
	body     []byte
	checksum string
}

type s3TestServer struct {
	t       *testing.T
	bucket  string
	server  *httptest.Server
	mu      sync.Mutex
	objects map[string]storedObject
	puts    []string
}

func newS3TestServer(t *testing.T, bucket string) *s3TestServer {
	t.Helper()
	store := &s3TestServer{t: t, bucket: bucket, objects: make(map[string]storedObject)}
	store.server = httptest.NewServer(http.HandlerFunc(store.serveHTTP))
	t.Cleanup(store.server.Close)
	return store
}

func (store *s3TestServer) URL() string {
	return store.server.URL
}

func (store *s3TestServer) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	prefix := "/" + store.bucket + "/"
	if !strings.HasPrefix(request.URL.Path, prefix) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	key, err := url.PathUnescape(strings.TrimPrefix(request.URL.Path, prefix))
	if err != nil {
		store.t.Errorf("unescape key: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	switch request.Method {
	case http.MethodPut:
		body, err := io.ReadAll(request.Body)
		if err != nil {
			store.t.Errorf("read put body: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		store.objects[key] = storedObject{body: body, checksum: request.Header.Get("X-Amz-Meta-Sha256")}
		store.puts = append(store.puts, key)
		writer.Header().Set("ETag", `"test"`)
		writer.WriteHeader(http.StatusOK)
	case http.MethodHead:
		object, exists := store.objects[key]
		if !exists {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(object.body)))
		writer.Header().Set("X-Amz-Meta-Sha256", object.checksum)
		writer.WriteHeader(http.StatusOK)
	case http.MethodGet:
		object, exists := store.objects[key]
		if !exists {
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(object.body)))
		writer.Header().Set("X-Amz-Meta-Sha256", object.checksum)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(object.body)
	case http.MethodDelete:
		delete(store.objects, key)
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (store *s3TestServer) putOrder() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]string(nil), store.puts...)
}

func (store *s3TestServer) body(key string) []byte {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]byte(nil), store.objects[key].body...)
}

func (store *s3TestServer) setChecksum(key, checksum string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	object := store.objects[key]
	object.checksum = checksum
	store.objects[key] = object
}

func newTestStore(t *testing.T, ctx context.Context, endpoint, bucket string) *rawstore.Store {
	t.Helper()
	store, err := rawstore.NewStore(ctx, rawstore.StoreConfig{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    bucket,
		AccessKey: "test-access",
		SecretKey: "test-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func stringPointer(value string) *string {
	return &value
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
