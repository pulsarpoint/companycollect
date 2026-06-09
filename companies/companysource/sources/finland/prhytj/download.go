package prhytj

import (
	"bytes"
	"context"
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
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

const (
	defaultFetchAttempts = 3
	fetchRetryDelay      = 100 * time.Millisecond
)

type downloadEnvelope struct {
	TotalResults *int64            `json:"totalResults"`
	Companies    []json.RawMessage `json:"companies"`
}

func (e *downloadEnvelope) UnmarshalJSON(data []byte) error {
	var raw struct {
		TotalResults *int64          `json:"totalResults"`
		Companies    json.RawMessage `json:"companies"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.Companies == nil {
		return errors.New("missing companies array")
	}

	companiesPayload := bytes.TrimSpace(raw.Companies)
	if bytes.Equal(companiesPayload, []byte("null")) {
		return errors.New("companies must be an array")
	}
	if len(companiesPayload) == 0 || companiesPayload[0] != '[' {
		return errors.New("companies must be an array")
	}

	var companies []json.RawMessage
	if err := json.Unmarshal(companiesPayload, &companies); err != nil {
		return err
	}

	e.TotalResults = raw.TotalResults
	e.Companies = companies
	return nil
}

func (s *Source) Download(ctx context.Context, opts sourcespec.DownloadOptions) (sourcespec.DownloadResult, error) {
	if s == nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil PRH YTJ source"),
		)
	}

	runDir := strings.TrimSpace(opts.RunDir)
	if runDir == "" {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			"",
			"",
			0,
			errors.New("run dir is required"),
		)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			runDir,
			0,
			errors.Wrap(err, "create run directory"),
		)
	}

	tempFile, err := os.CreateTemp(runDir, ".source.ndjson.*.tmp")
	if err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			runDir,
			0,
			errors.Wrap(err, "create temporary source file"),
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

	firstPage := countryimport.DefaultPageStart
	pageDelay := resolveDuration(0, s.cfg.PageDelay, countryimport.DefaultPageDelay)
	requestTimeout := resolveDuration(0, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(s.cfg.UserAgent, countryimport.DefaultUserAgent)
	maxPages := opts.MaxPages
	currentPage := firstPage

	var recordsSeen int64
	var totalPages int
	var totalResultsReported *int64

	for pagesRequested := 0; maxPages <= 0 || pagesRequested < maxPages; pagesRequested++ {
		pageURL, err := buildPageURL(s.cfg.BaseURL, currentPage, pagesRequested == 0)
		if err != nil {
			return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindInvalidConfig,
				SourceSlug,
				s.cfg.BaseURL,
				"",
				0,
				errors.Wrap(err, "build PRH page URL"),
			)
		}

		envelope, status, err := s.fetchPageWithRetries(ctx, pageURL, userAgent, requestTimeout)
		if err != nil {
			return sourcespec.DownloadResult{}, err
		}

		if envelope.TotalResults != nil && totalResultsReported == nil {
			totalResults := *envelope.TotalResults
			totalResultsReported = &totalResults
		}

		if len(envelope.Companies) == 0 {
			if isUnexpectedEarlyEmptyPage(totalResultsReported, recordsSeen) {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindRemoteDecode,
					SourceSlug,
					pageURL,
					"",
					status,
					errors.New("empty PRH companies page before totalResults reached"),
				)
			}
			break
		}
		if totalResultsReported != nil && totalPages == 0 {
			totalPages = computeTotalPages(*totalResultsReported, len(envelope.Companies))
		}

		for _, company := range envelope.Companies {
			line, err := compactCompanyLine(company)
			if err != nil {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindRemoteDecode,
					SourceSlug,
					pageURL,
					"",
					0,
					errors.Wrap(err, "compact PRH company line"),
				)
			}
			written, err := tempFile.Write(line)
			if err != nil {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO,
					SourceSlug,
					pageURL,
					tempPath,
					0,
					errors.Wrap(err, "write PRH company line"),
				)
			}
			if written != len(line) {
				return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO,
					SourceSlug,
					pageURL,
					tempPath,
					0,
					errors.Wrap(io.ErrShortWrite, "write PRH company line"),
				)
			}
			recordsSeen++
		}

		if totalPages > 0 && currentPage >= totalPages {
			break
		}
		if totalResultsReported != nil && *totalResultsReported > 0 && recordsSeen >= *totalResultsReported {
			break
		}
		if maxPages > 0 && pagesRequested+1 >= maxPages {
			break
		}
		currentPage++
		if err := waitPageDelay(ctx, pageDelay); err != nil {
			return sourcespec.DownloadResult{}, err
		}
	}

	if err := tempFile.Close(); err != nil {
		tempClosed = true
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			tempPath,
			0,
			errors.Wrap(err, "close temporary snapshot"),
		)
	}
	tempClosed = true

	finalPath := filepath.Join(runDir, "source.ndjson")
	if err := os.Rename(tempPath, finalPath); err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			finalPath,
			0,
			errors.Wrap(err, "rename temporary snapshot"),
		)
	}
	keepTemp = true

	result := sourcespec.DownloadResult{
		RunDir:      runDir,
		SourcePath:  finalPath,
		RecordsSeen: recordsSeen,
	}

	return result, nil
}

func (s *Source) fetchPageWithRetries(ctx context.Context, pageURL string, userAgent string, requestTimeout time.Duration) (downloadEnvelope, int, error) {
	var lastEnvelope downloadEnvelope
	var lastStatus int
	var lastErr error

	for attempt := 1; attempt <= defaultFetchAttempts; attempt++ {
		envelope, status, err := s.fetchPage(ctx, pageURL, userAgent, requestTimeout)
		if err == nil {
			return envelope, status, nil
		}
		lastEnvelope = envelope
		lastStatus = status
		lastErr = err

		if ctx.Err() != nil || attempt == defaultFetchAttempts || !isRetryableFetchError(err, status) {
			return lastEnvelope, lastStatus, lastErr
		}
		if err := waitFetchRetryDelay(ctx, pageURL); err != nil {
			return lastEnvelope, lastStatus, err
		}
	}

	return lastEnvelope, lastStatus, lastErr
}

func (s *Source) fetchPage(ctx context.Context, pageURL string, userAgent string, requestTimeout time.Duration) (downloadEnvelope, int, error) {
	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, pageURL, nil)
	if err != nil {
		return downloadEnvelope{}, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			pageURL,
			"",
			0,
			errors.Wrap(err, "create PRH page request"),
		)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}

	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		kind := countryimport.Classify(err)
		return downloadEnvelope{}, 0, countryimport.WrapSourceError(
			kind,
			SourceSlug,
			pageURL,
			"",
			0,
			errors.Wrap(err, "get PRH page"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return downloadEnvelope{}, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			pageURL,
			"",
			resp.StatusCode,
			errors.Newf("unexpected HTTP status %d", resp.StatusCode),
		)
	}

	var envelope downloadEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return downloadEnvelope{}, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			pageURL,
			"",
			resp.StatusCode,
			errors.Wrap(err, "decode PRH page"),
		)
	}

	return envelope, resp.StatusCode, nil
}

func isRetryableFetchError(err error, status int) bool {
	if err == nil {
		return false
	}
	if countryimport.Classify(err) == countryimport.ErrorKindTimeout {
		return true
	}
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func buildPageURL(baseURL string, page int, includeTotalResults bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	query.Set("page", strconv.Itoa(page))
	if includeTotalResults {
		query.Set("totalResults", "true")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func compactCompanyLine(company json.RawMessage) ([]byte, error) {
	line := bytes.Buffer{}
	if err := json.Compact(&line, company); err != nil {
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
			countryimport.Classify(ctx.Err()),
			SourceSlug,
			"",
			"",
			0,
			errors.Wrap(ctx.Err(), "wait between PRH pages"),
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
			countryimport.Classify(ctx.Err()),
			SourceSlug,
			pageURL,
			"",
			0,
			errors.Wrap(ctx.Err(), "wait before retrying PRH page"),
		)
	case <-timer.C:
		return nil
	}
}

func computeTotalPages(totalResults int64, pageSize int) int {
	if totalResults <= 0 || pageSize <= 0 {
		return 0
	}
	return int((totalResults + int64(pageSize) - 1) / int64(pageSize))
}

func isUnexpectedEarlyEmptyPage(totalResults *int64, recordsSeen int64) bool {
	if totalResults == nil || recordsSeen >= *totalResults {
		return false
	}
	return true
}

func resolveDuration(override time.Duration, configured time.Duration, fallback time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	if configured > 0 {
		return configured
	}
	return fallback
}

func resolveInt(override int, fallback int) int {
	if override > 0 {
		return override
	}
	return fallback
}

func resolveString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nilIfEmpty(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	return values
}
