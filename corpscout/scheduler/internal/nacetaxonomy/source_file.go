package nacetaxonomy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
)

const DefaultMaxSourceFileBytes int64 = 25 * 1024 * 1024

type DownloadedSourceFile struct {
	Body               []byte
	SHA256             string
	ContentLengthBytes int64
	ContentType        string
	ETag               string
	LastModified       string
}

func DownloadSourceFile(ctx context.Context, httpClient *http.Client, sourceURL string, maxBytes int64) (DownloadedSourceFile, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return DownloadedSourceFile{}, errors.New("nace source url is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSourceFileBytes
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return DownloadedSourceFile{}, errors.Wrap(err, "create nace source request")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return DownloadedSourceFile{}, errors.Wrap(err, "download nace source file")
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return DownloadedSourceFile{}, errors.Newf("download nace source file failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return DownloadedSourceFile{}, errors.Wrap(err, "read nace source file")
	}
	if int64(len(body)) > maxBytes {
		return DownloadedSourceFile{}, errors.New("nace source file exceeds maximum size")
	}

	sum := sha256.Sum256(body)
	return DownloadedSourceFile{
		Body:               body,
		SHA256:             hex.EncodeToString(sum[:]),
		ContentLengthBytes: int64(len(body)),
		ContentType:        resp.Header.Get("Content-Type"),
		ETag:               resp.Header.Get("ETag"),
		LastModified:       resp.Header.Get("Last-Modified"),
	}, nil
}
