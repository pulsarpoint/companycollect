package fetch

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"

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

// ObjectGetter supports both indexed record reads and streaming a complete WARC
// object to disk. S3 and anonymous HTTPS are the production implementations.
type ObjectGetter interface {
	RangeGetter
	ObjectSize(ctx context.Context, bucket, key string) (int64, error)
	DownloadObject(ctx context.Context, bucket, key string, destination *os.File) error
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
	embeddedHTTP, err := boundEmbeddedHTTP(record[:headerEnd], record[headerEnd+4:])
	if err != nil {
		return nil, nil, markParseError(errors.Wrap(err, "bound embedded HTTP response"))
	}
	response, strictErr := http.ReadResponse(bufio.NewReader(bytes.NewReader(embeddedHTTP)), nil)
	if strictErr != nil {
		var protocolErr textproto.ProtocolError
		if !errors.As(strictErr, &protocolErr) || !strings.HasPrefix(protocolErr.Error(), "malformed MIME header line:") {
			return nil, nil, markParseError(errors.Wrap(strictErr, "parse embedded HTTP response"))
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		sanitized, sanitizeErr := sanitizeEmbeddedHTTPHeaders(embeddedHTTP)
		if sanitizeErr != nil {
			err := errors.WithSecondaryError(
				errors.Wrap(strictErr, "parse embedded HTTP response"),
				errors.Wrap(sanitizeErr, "sanitize embedded HTTP headers"),
			)
			return nil, nil, markParseError(err)
		}
		response, err = http.ReadResponse(bufio.NewReader(bytes.NewReader(sanitized)), nil)
		if err != nil {
			err = errors.WithSecondaryError(
				errors.Wrap(err, "parse sanitized embedded HTTP response"),
				errors.Wrap(strictErr, "strict embedded HTTP parser"),
			)
			return nil, nil, markParseError(err)
		}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, markParseError(errors.Wrap(err, "read embedded HTTP body"))
	}
	return response.Header, body, nil
}

// boundEmbeddedHTTP uses the WARC Content-Length when present. Real WARC gzip members end with a
// record separator outside that length; excluding it keeps close-delimited HTTP bodies exact.
func boundEmbeddedHTTP(warcHeaders, remainder []byte) ([]byte, error) {
	var contentLength uint64
	hasContentLength := false
	for _, line := range bytes.Split(warcHeaders, []byte("\r\n")) {
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || !bytes.EqualFold(line[:colon], []byte("Content-Length")) {
			continue
		}
		value, err := strconv.ParseUint(string(bytes.TrimSpace(line[colon+1:])), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "invalid WARC Content-Length")
		}
		if hasContentLength && contentLength != value {
			return nil, errors.New("conflicting WARC Content-Length headers")
		}
		contentLength = value
		hasContentLength = true
	}
	if !hasContentLength {
		return remainder, nil
	}
	if contentLength > uint64(len(remainder)) {
		return nil, errors.Newf("short WARC content: got %d bytes, want %d", len(remainder), contentLength)
	}
	return remainder[:int(contentLength)], nil
}

// sanitizeEmbeddedHTTPHeaders drops only colon-bearing headers whose field name is invalid. It
// does not repair names, values, status lines, framing headers, or line endings. The body bytes are
// copied unchanged and net/http remains responsible for Content-Length and chunked decoding.
func sanitizeEmbeddedHTTPHeaders(response []byte) ([]byte, error) {
	headerEnd := bytes.Index(response, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return nil, errors.New("embedded HTTP header terminator not found")
	}

	lines := bytes.Split(response[:headerEnd], []byte("\r\n"))
	if len(lines) == 0 {
		return nil, errors.New("embedded HTTP status line not found")
	}
	statusLine := lines[0]
	if len(statusLine) == 0 {
		return nil, errors.New("embedded HTTP status line is empty")
	}

	var sanitized bytes.Buffer
	sanitized.Grow(len(response))
	sanitized.Write(statusLine)
	sanitized.WriteString("\r\n")
	previousHeaderKept := false
	droppedPreviousHeader := false
	droppedHeaders := 0
	for _, line := range lines[1:] {
		if len(line) == 0 {
			return nil, errors.New("unexpected empty line inside embedded HTTP headers")
		}
		if line[0] == ' ' || line[0] == '\t' {
			if droppedPreviousHeader {
				continue
			}
			if !previousHeaderKept || !validHTTPHeaderValue(line) {
				return nil, errors.New("invalid embedded HTTP folded header")
			}
			sanitized.Write(line)
			sanitized.WriteString("\r\n")
			continue
		}

		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return nil, errors.New("embedded HTTP header line has no field separator")
		}
		if !validHTTPHeaderName(line[:colon]) {
			if bodyFramingHeaderCandidate(line[:colon]) {
				return nil, errors.New("malformed embedded HTTP body-framing header")
			}
			previousHeaderKept = false
			droppedPreviousHeader = true
			droppedHeaders++
			continue
		}
		if !validHTTPHeaderValue(line[colon+1:]) {
			return nil, errors.New("invalid embedded HTTP header value")
		}
		sanitized.Write(line)
		sanitized.WriteString("\r\n")
		previousHeaderKept = true
		droppedPreviousHeader = false
	}
	if droppedHeaders == 0 {
		return nil, errors.New("no recoverable malformed embedded HTTP header found")
	}
	sanitized.WriteString("\r\n")
	sanitized.Write(response[headerEnd+4:])
	return sanitized.Bytes(), nil
}

func bodyFramingHeaderCandidate(name []byte) bool {
	normalized := make([]byte, 0, len(name))
	for _, character := range name {
		switch {
		case character >= 'A' && character <= 'Z':
			normalized = append(normalized, character+'a'-'A')
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			normalized = append(normalized, character)
		}
	}
	switch string(normalized) {
	case "contentlength", "transferencoding", "contentencoding":
		return true
	default:
		return false
	}
}

func validHTTPHeaderName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	for _, character := range name {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			continue
		default:
			return false
		}
	}
	return true
}

func validHTTPHeaderValue(value []byte) bool {
	for _, character := range value {
		if character == '\t' {
			continue
		}
		if character < ' ' || character == 0x7f {
			return false
		}
	}
	return true
}

func markParseError(err error) error {
	return errors.Mark(err, ErrParseRecord)
}
