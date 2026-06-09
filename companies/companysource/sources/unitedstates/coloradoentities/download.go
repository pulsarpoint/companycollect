package coloradoentities

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

const (
	defaultFetchAttempts = 3
	fetchRetryDelay      = 100 * time.Millisecond
)

// Download pages the SODA endpoint with $limit/$offset, writing one compact
// NDJSON record per entity. opts.MaxPages bounds the number of pages for smoke
// tests; otherwise paging stops when a short page is returned.
func (s *Source) Download(ctx context.Context, opts sourcespec.DownloadOptions) (sourcespec.DownloadResult, error) {
	if s == nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState, SourceSlug, "", "", 0,
			errors.New("nil Colorado entities source"),
		)
	}

	runDir := strings.TrimSpace(opts.RunDir)
	if runDir == "" {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig, SourceSlug, "", "", 0,
			errors.New("run dir is required"),
		)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", runDir, 0,
			errors.Wrap(err, "create Colorado run directory"),
		)
	}

	tempFile, err := os.CreateTemp(runDir, ".source.ndjson.*.tmp")
	if err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", runDir, 0,
			errors.Wrap(err, "create Colorado temporary snapshot"),
		)
	}
	tempPath := tempFile.Name()
	tempClosed := false
	keepTemp := false
	defer func() {
		if !tempClosed {
			_ = tempFile.Close()
		}
		if !keepTemp {
			_ = os.Remove(tempPath)
		}
	}()

	pageSize := s.cfg.PageSize
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	pageDelay := resolveDuration(0, s.cfg.PageDelay, countryimport.DefaultPageDelay)
	requestTimeout := resolveDuration(0, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(s.cfg.UserAgent, countryimport.DefaultUserAgent)
	maxPages := opts.MaxPages

	hasher := sha256.New()
	writer := io.MultiWriter(tempFile, hasher)
	httpStatuses := make(map[string]int)

	var bytesWritten int64
	var recordsSeen int64
	var pagesDownloaded int
	offset := 0

	for pagesRequested := 0; maxPages <= 0 || pagesRequested < maxPages; pagesRequested++ {
		pageURL, err := buildPageURL(s.cfg.BaseURL, pageSize, offset)
		if err != nil {
			return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindInvalidConfig, SourceSlug, s.cfg.BaseURL, "", 0,
				errors.Wrap(err, "build Colorado page URL"),
			)
		}

		entities, status, err := s.fetchPageWithRetries(ctx, pageURL, userAgent, requestTimeout)
		if status != 0 {
			httpStatuses[strconv.Itoa(offset)] = status
		}
		if err != nil {
			return sourcespec.DownloadResult{}, err
		}
		if len(entities) == 0 {
			break
		}

		pagesDownloaded++
		for _, entity := range entities {
			line, err := compactLine(entity)
			if err != nil {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindRemoteDecode, SourceSlug, pageURL, "", 0,
					errors.Wrap(err, "compact Colorado entity line"),
				)
			}
			written, err := writer.Write(line)
			if err != nil {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO, SourceSlug, pageURL, tempPath, 0,
					errors.Wrap(err, "write Colorado entity line"),
				)
			}
			if written != len(line) {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO, SourceSlug, pageURL, tempPath, 0,
					errors.Wrap(io.ErrShortWrite, "write Colorado entity line"),
				)
			}
			bytesWritten += int64(written)
			recordsSeen++
		}

		if len(entities) < pageSize {
			break
		}
		if maxPages > 0 && pagesRequested+1 >= maxPages {
			break
		}
		offset += pageSize
		if err := waitPageDelay(ctx, pageDelay); err != nil {
			return sourcespec.DownloadResult{}, err
		}
	}

	if err := tempFile.Close(); err != nil {
		tempClosed = true
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", tempPath, 0,
			errors.Wrap(err, "close Colorado temporary snapshot"),
		)
	}
	tempClosed = true

	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(runDir, "source.ndjson")
	if err := os.Rename(tempPath, finalPath); err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", finalPath, 0,
			errors.Wrap(err, "rename Colorado snapshot"),
		)
	}
	keepTemp = true

	_ = bytesWritten
	_ = pagesDownloaded
	_ = sha256Hex
	_ = httpStatuses
	return sourcespec.DownloadResult{RunDir: runDir, SourcePath: finalPath, RecordsSeen: recordsSeen}, nil
}

func (s *Source) fetchPageWithRetries(ctx context.Context, pageURL string, userAgent string, requestTimeout time.Duration) ([]json.RawMessage, int, error) {
	var lastEntities []json.RawMessage
	var lastStatus int
	var lastErr error

	for attempt := 1; attempt <= defaultFetchAttempts; attempt++ {
		entities, status, err := s.fetchPage(ctx, pageURL, userAgent, requestTimeout)
		if err == nil {
			return entities, status, nil
		}
		lastEntities = entities
		lastStatus = status
		lastErr = err

		if ctx.Err() != nil || attempt == defaultFetchAttempts || !isRetryableFetchError(err, status) {
			return lastEntities, lastStatus, lastErr
		}
		if err := waitFetchRetryDelay(ctx, pageURL); err != nil {
			return lastEntities, lastStatus, err
		}
	}

	return lastEntities, lastStatus, lastErr
}

func (s *Source) fetchPage(ctx context.Context, pageURL string, userAgent string, requestTimeout time.Duration) ([]json.RawMessage, int, error) {
	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig, SourceSlug, pageURL, "", 0,
			errors.Wrap(err, "create Colorado page request"),
		)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	if token := strings.TrimSpace(s.cfg.AppToken); token != "" {
		req.Header.Set("X-App-Token", token)
	}

	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, countryimport.WrapSourceError(
			countryimport.Classify(err), SourceSlug, pageURL, "", 0,
			errors.Wrap(err, "get Colorado page"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return nil, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus, SourceSlug, pageURL, "", resp.StatusCode,
			errors.Newf("unexpected Colorado HTTP status %d", resp.StatusCode),
		)
	}

	var entities []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&entities); err != nil {
		return nil, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode, SourceSlug, pageURL, "", resp.StatusCode,
			errors.Wrap(err, "decode Colorado page"),
		)
	}

	return entities, resp.StatusCode, nil
}

func isRetryableFetchError(err error, status int) bool {
	if err == nil {
		return false
	}
	if countryimport.Classify(err) == countryimport.ErrorKindTimeout {
		return true
	}
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func buildPageURL(baseURL string, limit int, offset int) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("$limit", strconv.Itoa(limit))
	query.Set("$offset", strconv.Itoa(offset))
	query.Set("$order", "entityid")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func compactLine(entity json.RawMessage) ([]byte, error) {
	line := bytes.Buffer{}
	if err := json.Compact(&line, entity); err != nil {
		return nil, err
	}
	line.WriteByte('\n')
	return line.Bytes(), nil
}

func waitPageDelay(ctx context.Context, pageDelay time.Duration) error {
	if pageDelay <= 0 {
		return nil
	}
	timer := time.NewTimer(pageDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return countryimport.WrapSourceError(
			countryimport.Classify(ctx.Err()), SourceSlug, "", "", 0,
			errors.Wrap(ctx.Err(), "wait between Colorado pages"),
		)
	case <-timer.C:
		return nil
	}
}

func waitFetchRetryDelay(ctx context.Context, pageURL string) error {
	timer := time.NewTimer(fetchRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return countryimport.WrapSourceError(
			countryimport.Classify(ctx.Err()), SourceSlug, pageURL, "", 0,
			errors.Wrap(ctx.Err(), "wait before retrying Colorado page"),
		)
	case <-timer.C:
		return nil
	}
}
