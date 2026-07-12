package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeGetter struct{ data []byte }

func (f fakeGetter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	return f.data, nil
}

func TestFetchRecord(t *testing.T) {
	body := "<html>wp-content here</html>"
	record := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: 999\r\n\r\n" +
		"HTTP/1.1 200 OK\r\nServer: nginx\r\nContent-Type: text/html\r\n" +
		"Content-Length: " + itoa(len(body)) + "\r\n\r\n" + body
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write([]byte(record))
	gw.Close()

	headers, gotBody, err := FetchRecord(context.Background(), fakeGetter{data: buf.Bytes()}, "b", "k", 0, int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if headers.Get("Server") != "nginx" {
		t.Fatalf("server=%q", headers.Get("Server"))
	}
	if !bytes.Contains(gotBody, []byte("wp-content")) {
		t.Fatalf("body=%q", gotBody)
	}
}

func TestS3GetterRetriesInterruptedBodyRead(t *testing.T) {
	const content = "abcdef"
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		w.Header().Set("Content-Range", "bytes 0-5/6")
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusPartialContent)
		if request == 1 {
			_, _ = io.WriteString(w, content[:3])
			return
		}
		_, _ = io.WriteString(w, content)
	}))
	defer server.Close()

	stats := &s3Counters{}
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(server.URL),
		Region:       "us-east-1",
		Credentials:  aws.AnonymousCredentials{},
		HTTPClient: &http.Client{Transport: s3StatsTransport{
			base:  server.Client().Transport,
			stats: stats,
		}},
		Retryer:      aws.NopRetryer{},
		UsePathStyle: true,
	})
	getter := &S3Getter{Client: client, stats: stats}

	got, err := getter.GetRange(context.Background(), "bucket", "key", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("body=%q, want %q", got, content)
	}

	gotStats := getter.Stats()
	if gotStats.GetObjectCalls != 2 || gotStats.HTTPAttempts != 2 {
		t.Fatalf("calls=%d HTTP attempts=%d, want 2 each", gotStats.GetObjectCalls, gotStats.HTTPAttempts)
	}
	if gotStats.BodyReadAttempts != 2 || gotStats.BodyReadErrors != 1 || gotStats.BodyReadRetries != 1 {
		t.Fatalf("body stats=%+v, want attempts=2 errors=1 retries=1", gotStats)
	}
	if gotStats.BodyBytes != 9 {
		t.Fatalf("body bytes=%d, want 9 including the interrupted attempt", gotStats.BodyBytes)
	}
}

func TestS3GetterGetsSizeAndStreamsObjectWithRetry(t *testing.T) {
	const content = "complete WARC object"
	var downloads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", itoa(len(content)))
		case http.MethodGet:
			download := downloads.Add(1)
			w.Header().Set("Content-Length", itoa(len(content)))
			if download == 1 {
				_, _ = io.WriteString(w, content[:5])
				return
			}
			_, _ = io.WriteString(w, content)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	stats := &s3Counters{}
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(server.URL),
		Region:       "us-east-1",
		Credentials:  aws.AnonymousCredentials{},
		HTTPClient: &http.Client{Transport: s3StatsTransport{
			base:  server.Client().Transport,
			stats: stats,
		}},
		Retryer:      aws.NopRetryer{},
		UsePathStyle: true,
	})
	getter := &S3Getter{Client: client, stats: stats}

	size, err := getter.ObjectSize(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(content)) {
		t.Fatalf("size=%d, want %d", size, len(content))
	}

	destination, err := os.CreateTemp(t.TempDir(), "warc-*.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if _, err := destination.WriteString("old data that must be removed"); err != nil {
		t.Fatal(err)
	}
	if err := getter.DownloadObject(context.Background(), "bucket", "key", destination); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("downloaded body=%q, want %q", got, content)
	}

	gotStats := getter.Stats()
	if gotStats.HeadObjectCalls != 1 || gotStats.GetObjectCalls != 2 || gotStats.HTTPAttempts != 3 {
		t.Fatalf("HEAD calls=%d GET calls=%d HTTP attempts=%d, want 1/2/3", gotStats.HeadObjectCalls, gotStats.GetObjectCalls, gotStats.HTTPAttempts)
	}
	if gotStats.BodyReadAttempts != 2 || gotStats.BodyReadErrors != 1 || gotStats.BodyReadRetries != 1 {
		t.Fatalf("body stats=%+v, want attempts=2 errors=1 retries=1", gotStats)
	}
	if gotStats.BodyBytes != int64(5+len(content)) {
		t.Fatalf("body bytes=%d, want %d", gotStats.BodyBytes, 5+len(content))
	}
}

func TestHTTPGetterGetsSizeAndStreamsObjectWithRetry(t *testing.T) {
	const content = "complete WARC object"
	var downloads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", itoa(len(content)))
		case http.MethodGet:
			download := downloads.Add(1)
			w.Header().Set("Content-Length", itoa(len(content)))
			if download == 1 {
				_, _ = io.WriteString(w, content[:5])
				return
			}
			_, _ = io.WriteString(w, content)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	getter := NewHTTPGetter(server.URL, 1)
	size, err := getter.ObjectSize(context.Background(), "ignored", "object.warc.gz")
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(content)) {
		t.Fatalf("size=%d, want %d", size, len(content))
	}

	destination, err := os.CreateTemp(t.TempDir(), "warc-*.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if _, err := destination.WriteString("stale bytes"); err != nil {
		t.Fatal(err)
	}
	if err := getter.DownloadObject(context.Background(), "ignored", "object.warc.gz", destination); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("downloaded body=%q, want %q", got, content)
	}
	if downloads.Load() != 2 {
		t.Fatalf("downloads=%d, want one interrupted attempt and one retry", downloads.Load())
	}
}

func TestHTTPGetterDoesNotRetryPermanentDownloadFailure(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	destination, err := os.CreateTemp(t.TempDir(), "warc-*.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	err = NewHTTPGetter(server.URL, 1).DownloadObject(context.Background(), "ignored", "missing.warc.gz", destination)
	if err == nil {
		t.Fatal("expected permanent HTTP error")
	}
	if requests.Load() != 1 {
		t.Fatalf("requests=%d, want 1", requests.Load())
	}
}

func TestHTTPGetterRejectsServerThatIgnoresRange(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "complete object that must not be accepted as one record")
	}))
	defer server.Close()

	_, err := NewHTTPGetter(server.URL, 1).GetRange(context.Background(), "ignored", "object.warc.gz", 0, 5)
	if err == nil || !strings.Contains(err.Error(), "ignored bytes=0-5") {
		t.Fatalf("error=%v, want ignored-range failure", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests=%d, want no retries for HTTP 200", requests.Load())
	}
}

func TestHTTPGetterRetriesInterruptedRangeBody(t *testing.T) {
	const content = "abcdef"
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		w.Header().Set("Content-Range", "bytes 0-5/100")
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusPartialContent)
		if request == 1 {
			_, _ = io.WriteString(w, content[:3])
			return
		}
		_, _ = io.WriteString(w, content)
	}))
	defer server.Close()

	got, err := NewHTTPGetter(server.URL, 1).GetRange(context.Background(), "ignored", "object.warc.gz", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("range=%q, want %q", got, content)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests=%d, want one interrupted attempt and one retry", requests.Load())
	}
}

func TestS3StatsTransportCountsThrottleResponses(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			stats := &s3Counters{}
			transport := s3StatsTransport{
				base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: status,
						Body:       http.NoBody,
						Header:     make(http.Header),
						Request:    req,
					}, nil
				}),
				stats: stats,
			}
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}

			got := (&S3Getter{stats: stats}).Stats()
			if got.HTTPAttempts != 1 || got.HTTP429s+got.HTTP503s != 1 {
				t.Fatalf("stats=%+v, want one HTTP attempt and one throttle response", got)
			}
			if got.HTTPHeaderTime <= 0 {
				t.Fatalf("HTTP header time=%s, want positive duration", got.HTTPHeaderTime)
			}
		})
	}
}

func TestS3StatsDelta(t *testing.T) {
	previous := S3Stats{HeadObjectCalls: 1, HeadObjectTime: time.Millisecond, GetObjectCalls: 2, GetObjectTime: 3 * time.Millisecond, HTTP429s: 1, BodyBytes: 10}
	current := S3Stats{HeadObjectCalls: 3, HeadObjectTime: 4 * time.Millisecond, GetObjectCalls: 5, GetObjectTime: 8 * time.Millisecond, HTTP429s: 3, BodyBytes: 24}

	delta := current.Delta(previous)
	if delta.HeadObjectCalls != 2 || delta.HeadObjectTime != 3*time.Millisecond ||
		delta.GetObjectCalls != 3 || delta.GetObjectTime != 5*time.Millisecond || delta.HTTP429s != 2 || delta.BodyBytes != 14 {
		t.Fatalf("delta=%+v", delta)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
