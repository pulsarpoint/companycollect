package stagedinput

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cc-raw/rawstore"
	"github.com/parquet-go/parquet-go"
)

func TestOpenValidatesRustFSPartAndServesOriginalWARCRanges(t *testing.T) {
	const (
		crawlID   = "CC-MAIN-2026-25"
		selection = "pages25"
		part      = 0
	)
	packBody := []byte("complete compressed WARC record")
	packOffset := int64(0)
	packLength := int64(len(packBody))
	recordChecksum := string(rawstore.ChecksumBytes(packBody))
	failureCode := "timeout"
	rows := []rawstore.IndexRow{{
		WorklistOrdinal:  0,
		DomainRank:       0,
		RootDomain:       "example.com",
		URL:              "https://example.com/",
		IsPrimary:        true,
		WARCFilename:     "crawl-data/source.warc.gz",
		WARCOffset:       100,
		WARCLength:       int64(len(packBody)),
		DownloadStatus:   rawstore.Downloaded,
		DownloadAttempts: 1,
		PackOffset:       &packOffset,
		PackLength:       &packLength,
		RecordChecksum:   &recordChecksum,
	}, {
		WorklistOrdinal:  1,
		DomainRank:       1,
		RootDomain:       "failed.example",
		URL:              "https://failed.example/",
		IsPrimary:        true,
		WARCFilename:     "crawl-data/failed.warc.gz",
		WARCOffset:       200,
		WARCLength:       50,
		DownloadStatus:   rawstore.Failed,
		DownloadAttempts: 3,
		ErrorCode:        &failureCode,
	}}
	var indexBuffer bytes.Buffer
	if err := parquet.Write(&indexBuffer, rows); err != nil {
		t.Fatal(err)
	}
	indexBody := indexBuffer.Bytes()
	keys, err := rawstore.KeysForChunk(crawlID, selection, part, 0)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	manifest := rawstore.ChunkManifest{
		SchemaVersion: rawstore.SchemaVersion,
		CrawlID:       crawlID,
		Selection:     selection,
		Part:          part,
		Chunk:         0,
		Worklist: rawstore.ChunkWorklist{
			Key: "download/worklists/pages25/part_000.parquet", SHA256: rawstore.SHA256(strings.Repeat("a", 64)),
			FirstOrdinal: 0, RecordCount: 2,
		},
		Pack:  rawstore.ObjectDescriptor{Key: keys.Pack, SizeBytes: int64(len(packBody)), SHA256: rawstore.ChecksumBytes(packBody)},
		Index: rawstore.ObjectDescriptor{Key: keys.Index, SizeBytes: int64(len(indexBody)), SHA256: rawstore.ChecksumBytes(indexBody)},
		Results: rawstore.DownloadResults{
			RequestedRecords: 2, DownloadedRecords: 1, FailedRecords: 1,
			SourceBytes: int64(len(packBody)), PackedBytes: int64(len(packBody)),
			Errors:         rawstore.DownloadErrorCounts{Timeout: 1},
			FailureReasons: map[string]int64{"timeout": 1},
		},
		Download: rawstore.DownloadRun{
			RunID: "download-run", WorkerHost: "download-host", GitCommit: "commit",
			StartedAt: now, CompletedAt: now.Add(time.Second),
		},
	}
	if err := rawstore.ValidateIndexRows(rows, manifest); err != nil {
		t.Fatal(err)
	}
	manifestBody, err := rawstore.EncodeChunkManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	ready := rawstore.ReadyManifest{
		SchemaVersion: rawstore.SchemaVersion,
		CrawlID:       crawlID,
		Selection:     selection,
		Part:          part,
		Worklist: rawstore.ReadyWorklist{
			Key: manifest.Worklist.Key, SizeBytes: 100, SHA256: manifest.Worklist.SHA256, RecordCount: 2,
		},
		Chunks: []rawstore.ReadyChunk{{
			Chunk: 0, ManifestKey: keys.Manifest, ManifestSHA256: rawstore.ChecksumBytes(manifestBody),
			FirstOrdinal: 0, RecordCount: 2,
			RawBytes: int64(len(packBody) + len(indexBody) + len(manifestBody)),
		}},
		Totals: rawstore.ReadyTotals{
			ChunkCount: 1, RequestedRecords: 2, DownloadedRecords: 1, FailedRecords: 1,
			RawBytes: int64(len(packBody) + len(indexBody) + len(manifestBody)),
		},
		DownloadRunID: "download-run",
		CompletedAt:   now.Add(time.Second),
	}
	readyBody, err := rawstore.EncodeReadyManifest(ready)
	if err != nil {
		t.Fatal(err)
	}
	readyKey := fmt.Sprintf("commoncrawl/state/crawl=%s/selection=%s/part=%03d/download/ready.json", crawlID, selection, part)
	objects := map[string][]byte{
		readyKey: readyBody, keys.Manifest: manifestBody, keys.Index: indexBody, keys.Pack: packBody,
	}
	server := newObjectServer(t, "crawls", objects)
	defer server.Close()
	store, err := rawstore.NewStore(context.Background(), rawstore.StoreConfig{
		Endpoint: server.URL, Region: "us-east-1", Bucket: "crawls", AccessKey: "access", SecretKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	cacheDirectory := filepath.Join(t.TempDir(), "raw-input")
	input, err := Open(context.Background(), store, crawlID, selection, part, cacheDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Items) != 1 || input.Items[0].WarcFilename != rows[0].WARCFilename || input.Items[0].Offset != rows[0].WARCOffset {
		t.Fatalf("unexpected input items %+v", input.Items)
	}
	if input.RequestedRecords != 2 || input.FailedRecords != 1 {
		t.Fatalf("input totals requested=%d failed=%d", input.RequestedRecords, input.FailedRecords)
	}
	got, err := input.Getter.GetRange(context.Background(), "commoncrawl", rows[0].WARCFilename, rows[0].WARCOffset, rows[0].WARCOffset+rows[0].WARCLength-1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, packBody) {
		t.Fatalf("record body=%q, want %q", got, packBody)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cacheDirectory); !os.IsNotExist(err) {
		t.Fatalf("input cache remains after close: %v", err)
	}
}

func newObjectServer(t *testing.T, bucket string, objects map[string][]byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		prefix := "/" + bucket + "/"
		if request.Method != http.MethodGet || !strings.HasPrefix(request.URL.Path, prefix) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		key, err := url.PathUnescape(strings.TrimPrefix(request.URL.Path, prefix))
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		body, exists := objects[key]
		if !exists {
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusNotFound)
			_, _ = writer.Write([]byte("<Error><Code>NoSuchKey</Code></Error>"))
			return
		}
		writer.Header().Set("Content-Length", fmt.Sprint(len(body)))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(body)
	}))
}
