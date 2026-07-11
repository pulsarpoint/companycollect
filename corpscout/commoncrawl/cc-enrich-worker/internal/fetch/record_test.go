package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
)

type recordingGetter struct {
	data       []byte
	err        error
	start, end int64
	calls      int
}

func (getter *recordingGetter) GetRange(_ context.Context, _, _ string, start, end int64) ([]byte, error) {
	getter.calls++
	getter.start = start
	getter.end = end
	return getter.data, getter.err
}

func TestFetchRawRecordReturnsExactRangeBytes(t *testing.T) {
	want := []byte{0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef}
	getter := &recordingGetter{data: want}

	got, err := FetchRawRecord(context.Background(), getter, "bucket", "warc.gz", 100, int64(len(want)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("raw bytes changed: got %x want %x", got, want)
	}
	if getter.calls != 1 || getter.start != 100 || getter.end != 107 {
		t.Fatalf("range call=%d start=%d end=%d", getter.calls, getter.start, getter.end)
	}
}

func TestFetchRawRecordRejectsShortResponse(t *testing.T) {
	getter := &recordingGetter{data: []byte("short")}
	_, err := FetchRawRecord(context.Background(), getter, "bucket", "warc.gz", 0, 10)
	if err == nil || !errors.Is(err, ErrFetchRecord) {
		t.Fatalf("expected fetch-stage error, got %v", err)
	}
}

func TestFetchRawRecordPreservesGetterError(t *testing.T) {
	sourceErr := errors.New("remote timeout")
	getter := &recordingGetter{err: sourceErr}
	_, err := FetchRawRecord(context.Background(), getter, "bucket", "warc.gz", 20, 30)
	if err == nil || !errors.Is(err, ErrFetchRecord) || !errors.Is(err, sourceErr) {
		t.Fatalf("expected fetch and source errors, got %v", err)
	}
	if errors.Is(err, ErrParseRecord) {
		t.Fatalf("fetch error was marked as parse error: %v", err)
	}
}

func TestParseRecordReturnsEmbeddedHTTPResponse(t *testing.T) {
	body := []byte("<html>technology</html>")
	raw := compressedWARCResponse(t, "Server: nginx\r\nContent-Type: text/html\r\n", body)

	headers, gotBody, err := ParseRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if headers.Get("Server") != "nginx" {
		t.Fatalf("server header=%q", headers.Get("Server"))
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body=%q, want %q", gotBody, body)
	}
}

func TestParseRecordClassifiesMalformedRecords(t *testing.T) {
	tests := map[string][]byte{
		"not gzip":            []byte("not a gzip member"),
		"missing WARC end":    compressedBytes(t, []byte("WARC/1.0\r\nWARC-Type: response")),
		"malformed HTTP":      compressedBytes(t, []byte("WARC/1.0\r\n\r\nHTTP/1.1 200 OK\r\nBad Header\r\n\r\n")),
		"truncated gzip body": truncatedCompressedBytes(t, []byte("WARC/1.0\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := ParseRecord(raw)
			if err == nil || !errors.Is(err, ErrParseRecord) {
				t.Fatalf("expected parse-stage error, got %v", err)
			}
			if errors.Is(err, ErrFetchRecord) {
				t.Fatalf("parse error was marked as fetch error: %v", err)
			}
		})
	}
}

func TestFetchRecordMatchesSeparateFetchAndParse(t *testing.T) {
	body := []byte("<html>same output</html>")
	raw := compressedWARCResponse(t, "Content-Type: text/html\r\n", body)

	wantHeaders, wantBody, err := ParseRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	getter := &recordingGetter{data: raw}
	gotHeaders, gotBody, err := FetchRecord(context.Background(), getter, "bucket", "warc.gz", 40, int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBody, wantBody) || gotHeaders.Get("Content-Type") != wantHeaders.Get("Content-Type") {
		t.Fatalf("composed result differs: headers=%v body=%q", gotHeaders, gotBody)
	}
}

func TestFetchRecordKeepsFetchAndParseFailuresDistinct(t *testing.T) {
	sourceErr := errors.New("connection reset")
	_, _, fetchErr := FetchRecord(context.Background(), &recordingGetter{err: sourceErr}, "bucket", "warc.gz", 0, 10)
	if !errors.Is(fetchErr, ErrFetchRecord) || errors.Is(fetchErr, ErrParseRecord) {
		t.Fatalf("unexpected fetch error classification: %v", fetchErr)
	}

	malformed := []byte("not gzip")
	_, _, parseErr := FetchRecord(context.Background(), &recordingGetter{data: malformed}, "bucket", "warc.gz", 0, int64(len(malformed)))
	if !errors.Is(parseErr, ErrParseRecord) || errors.Is(parseErr, ErrFetchRecord) {
		t.Fatalf("unexpected parse error classification: %v", parseErr)
	}
}

func compressedWARCResponse(t *testing.T, headers string, body []byte) []byte {
	t.Helper()
	record := []byte("WARC/1.0\r\nWARC-Type: response\r\n\r\n" +
		"HTTP/1.1 200 OK\r\n" + headers + "Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + string(body))
	return compressedBytes(t, record)
}

func compressedBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func truncatedCompressedBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	compressed := compressedBytes(t, data)
	return compressed[:len(compressed)-4]
}
