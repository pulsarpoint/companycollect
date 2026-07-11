package warcsize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestMeasureUsesCacheAndRangeMetadataFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodHead {
			writer.WriteHeader(http.StatusOK)
			return
		}
		if request.Header.Get("Range") != "bytes=0-0" {
			t.Fatalf("unexpected range %q", request.Header.Get("Range"))
		}
		writer.Header().Set("Content-Range", "bytes 0-0/2000")
		writer.WriteHeader(http.StatusPartialContent)
	}))
	defer server.Close()

	cache := Cache{"cached.warc.gz": 1000}
	summary, err := Measure(
		context.Background(),
		[]string{"cached.warc.gz", "measured.warc.gz"},
		server.URL,
		2,
		cache,
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Objects != 2 || summary.Bytes != 3000 || summary.CacheHits != 1 || summary.HTTPChecks != 1 {
		t.Fatalf("unexpected summary %+v", summary)
	}
	if cache["measured.warc.gz"] != 2000 {
		t.Fatalf("measured size was not cached: %+v", cache)
	}

	path := filepath.Join(t.TempDir(), "cache", "warc-sizes.json")
	if err := SaveCache(path, cache); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCache(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded["cached.warc.gz"] != 1000 || loaded["measured.warc.gz"] != 2000 {
		t.Fatalf("unexpected loaded cache %+v", loaded)
	}
}

func TestContentRangeSizeRejectsInvalidValues(t *testing.T) {
	if size, err := contentRangeSize("bytes 0-0/1234"); err != nil || size != 1234 {
		t.Fatalf("size=%d err=%v", size, err)
	}
	for _, value := range []string{"", "bytes 0-0/*", "bytes 0-0/nope"} {
		if _, err := contentRangeSize(value); err == nil {
			t.Fatalf("Content-Range %q unexpectedly succeeded", value)
		}
	}
}
