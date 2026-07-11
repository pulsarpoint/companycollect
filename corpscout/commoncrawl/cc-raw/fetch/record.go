package fetch

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"

	"github.com/cockroachdb/errors"
)

var (
	ErrFetchRecord = errors.New("WARC record fetch failed")
	ErrParseRecord = errors.New("WARC record parse failed")
)

// RangeGetter fetches a byte range of an object. S3 and anonymous HTTPS are
// the production implementations; tests use a focused fake at this protocol boundary.
type RangeGetter interface {
	GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

// FetchRecord preserves the original fetch-and-parse API while the downloader and
// staged processors use FetchRawRecord and ParseRecord independently.
func FetchRecord(ctx context.Context, getter RangeGetter, bucket, key string, offset, length int64) (http.Header, []byte, error) {
	raw, err := FetchRawRecord(ctx, getter, bucket, key, offset, length)
	if err != nil {
		return nil, nil, err
	}
	return ParseRecord(raw)
}

// FetchRawRecord returns the exact compressed WARC gzip member selected by the
// Common Crawl index. A short response is an error because it cannot be safely packed.
func FetchRawRecord(ctx context.Context, getter RangeGetter, bucket, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 {
		return nil, errors.Mark(errors.Newf("invalid WARC range offset=%d length=%d", offset, length), ErrFetchRecord)
	}
	end := offset + length - 1
	if end < offset {
		return nil, errors.Mark(errors.Newf("WARC range overflows offset=%d length=%d", offset, length), ErrFetchRecord)
	}
	raw, err := getter.GetRange(ctx, bucket, key, offset, end)
	if err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "fetch WARC range bucket=%s key=%s offset=%d length=%d", bucket, key, offset, length), ErrFetchRecord)
	}
	if int64(len(raw)) != length {
		return nil, errors.Mark(errors.Newf("short WARC range bucket=%s key=%s: got %d bytes, want %d", bucket, key, len(raw), length), ErrFetchRecord)
	}
	return raw, nil
}

// ParseRecord decompresses one WARC gzip member and returns its embedded HTTP
// headers and body. It performs no network or object-store I/O.
func ParseRecord(raw []byte) (http.Header, []byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, markParseError(errors.Wrap(err, "open WARC gzip member"))
	}
	defer gzipReader.Close()

	record, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, nil, markParseError(errors.Wrap(err, "decompress WARC record"))
	}
	headerEnd := bytes.Index(record, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return nil, nil, markParseError(errors.New("WARC header terminator not found"))
	}
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(record[headerEnd+4:])), nil)
	if err != nil {
		return nil, nil, markParseError(errors.Wrap(err, "parse embedded HTTP response"))
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, markParseError(errors.Wrap(err, "read embedded HTTP body"))
	}
	return response.Header, body, nil
}

func markParseError(err error) error {
	return errors.Mark(err, ErrParseRecord)
}
