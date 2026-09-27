package feeds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testFetcher(srv *httptest.Server) *Fetcher {
	return &Fetcher{Client: srv.Client(), Retries: 2, Backoff: time.Millisecond}
}

func TestFetcherRetriesServerErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != userAgent {
			t.Errorf("user agent = %q", r.Header.Get("User-Agent"))
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", `"abc"`)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	resp, err := testFetcher(srv).Get(context.Background(), srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "ok" || resp.ETag != "abc" || calls.Load() != 3 {
		t.Fatalf("body=%q etag=%q calls=%d", resp.Body, resp.ETag, calls.Load())
	}
}

func TestFetcherDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := testFetcher(srv).Get(context.Background(), srv.URL, ""); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}
