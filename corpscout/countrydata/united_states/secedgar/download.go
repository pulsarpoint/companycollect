package secedgar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

// Download fetches the SEC company_tickers.json document and flattens its
// index-keyed object into a one-record-per-line NDJSON snapshot. The endpoint is
// a single, unpaginated document, so DownloadOptions paging fields are ignored.
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error) {
	if s == nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil SEC EDGAR source"),
		)
	}

	startedAt := time.Now().UTC()
	dataDir := resolveString(opts.DataDir, s.cfg.DataDir, defaultDataDir)
	snapshotsDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotsDir, 0o755); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotsDir,
			0,
			errors.Wrap(err, "create snapshots directory"),
		)
	}

	requestTimeout := resolveDuration(opts.RequestTimeout, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(opts.UserAgent, s.cfg.UserAgent, countryimport.DefaultUserAgent)
	downloadURL := s.cfg.DownloadURL

	file, status, err := s.fetchTickerFile(ctx, downloadURL, userAgent, requestTimeout)
	httpStatuses := make(map[string]int)
	if status != 0 {
		httpStatuses["1"] = status
	}
	if err != nil {
		return countryimport.DownloadResult{}, err
	}

	tempFile, err := os.CreateTemp(snapshotsDir, ".sec_company_tickers_*.ndjson.tmp")
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotsDir,
			0,
			errors.Wrap(err, "create temporary snapshot"),
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

	hasher := sha256.New()
	writer := io.MultiWriter(tempFile, hasher)

	var bytesDownloaded int64
	var recordsSeen int64
	for _, company := range file.Records {
		line, err := compactCompanyLine(company)
		if err != nil {
			return countryimport.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindRemoteDecode,
				SourceSlug,
				downloadURL,
				"",
				0,
				errors.Wrap(err, "compact SEC company line"),
			)
		}
		written, err := writer.Write(line)
		if err != nil {
			return countryimport.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO,
				SourceSlug,
				downloadURL,
				tempPath,
				0,
				errors.Wrap(err, "write SEC company line"),
			)
		}
		if written != len(line) {
			return countryimport.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO,
				SourceSlug,
				downloadURL,
				tempPath,
				0,
				errors.Wrap(io.ErrShortWrite, "write SEC company line"),
			)
		}
		bytesDownloaded += int64(written)
		recordsSeen++
	}

	if err := tempFile.Close(); err != nil {
		tempClosed = true
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			tempPath,
			0,
			errors.Wrap(err, "close temporary snapshot"),
		)
	}
	tempClosed = true

	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(
		snapshotsDir,
		"sec_company_tickers_"+startedAt.Format("20060102T150405.000000000Z")+".ndjson",
	)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			finalPath,
			0,
			errors.Wrap(err, "rename temporary snapshot"),
		)
	}
	keepTemp = true

	finishedAt := time.Now().UTC()
	result := countryimport.DownloadResult{
		SourceSlug:      SourceSlug,
		SnapshotPath:    finalPath,
		BytesDownloaded: bytesDownloaded,
		RecordsSeen:     recordsSeen,
		PagesDownloaded: 1,
		SHA256:          sha256Hex,
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		Duration:        finishedAt.Sub(startedAt),
	}
	metadata := countryimport.DownloadMetadata{
		SourceSlug:      result.SourceSlug,
		SourceName:      SourceName,
		BaseURL:         downloadURL,
		SnapshotPath:    result.SnapshotPath,
		StartedAt:       result.StartedAt,
		FinishedAt:      result.FinishedAt,
		DurationMS:      result.Duration.Milliseconds(),
		BytesDownloaded: result.BytesDownloaded,
		RecordsSeen:     result.RecordsSeen,
		PagesDownloaded: result.PagesDownloaded,
		FirstPage:       1,
		LastPage:        1,
		SHA256:          result.SHA256,
		HTTPStatuses:    nilIfEmpty(httpStatuses),
		License:         "U.S. Government work / public domain",
		Attribution:     "U.S. Securities and Exchange Commission (SEC) EDGAR",
	}

	if err := s.saveDownloadMetadata(ctx, metadata); err != nil {
		return result, err
	}

	return result, nil
}

func (s *Source) fetchTickerFile(ctx context.Context, downloadURL string, userAgent string, requestTimeout time.Duration) (tickerFile, int, error) {
	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return tickerFile{}, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			downloadURL,
			"",
			0,
			errors.Wrap(err, "create SEC ticker request"),
		)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	req.Header.Set("Accept", "application/json")

	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		kind := countryimport.Classify(err)
		return tickerFile{}, 0, countryimport.WrapSourceError(
			kind,
			SourceSlug,
			downloadURL,
			"",
			0,
			errors.Wrap(err, "get SEC ticker file"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return tickerFile{}, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			downloadURL,
			"",
			resp.StatusCode,
			errors.Newf("unexpected HTTP status %d", resp.StatusCode),
		)
	}

	var file tickerFile
	if err := json.NewDecoder(resp.Body).Decode(&file); err != nil {
		return tickerFile{}, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			downloadURL,
			"",
			resp.StatusCode,
			errors.Wrap(err, "decode SEC ticker file"),
		)
	}

	return file, resp.StatusCode, nil
}

func compactCompanyLine(company json.RawMessage) ([]byte, error) {
	line := bytes.Buffer{}
	if err := json.Compact(&line, company); err != nil {
		return nil, err
	}
	line.WriteByte('\n')
	return line.Bytes(), nil
}

func (s *Source) saveDownloadMetadata(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	s.latestDownload = &metadata
	metadataStore := s.metadataStore
	if metadataStore == nil {
		metadataStore = countryimport.NoopMetadataStore{}
	}
	if err := metadataStore.SaveDownload(ctx, metadata); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			metadata.SnapshotPath,
			0,
			errors.Wrap(err, "save download metadata"),
		)
	}
	return nil
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
