package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestS3StatsTransportCounts503(t *testing.T) {
	stats := &s3Counters{}
	transport := s3StatsTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
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

	getter := &S3Getter{stats: stats}
	got := getter.Stats()
	if got.HTTPAttempts != 1 || got.HTTP503s != 1 {
		t.Fatalf("stats=%+v, want one HTTP attempt and one 503", got)
	}
	if got.HTTPHeaderTime <= 0 {
		t.Fatalf("HTTP header time=%s, want positive duration", got.HTTPHeaderTime)
	}
}

func TestS3StatsDelta(t *testing.T) {
	previous := S3Stats{GetObjectCalls: 2, GetObjectTime: 3 * time.Millisecond, BodyBytes: 10}
	current := S3Stats{GetObjectCalls: 5, GetObjectTime: 8 * time.Millisecond, BodyBytes: 24}

	delta := current.Delta(previous)
	if delta.GetObjectCalls != 3 || delta.GetObjectTime != 5*time.Millisecond || delta.BodyBytes != 14 {
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
