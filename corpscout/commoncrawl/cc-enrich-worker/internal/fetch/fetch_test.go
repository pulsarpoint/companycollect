package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"
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
