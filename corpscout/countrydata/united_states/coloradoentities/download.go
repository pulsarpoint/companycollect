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
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

// Download pages the Socrata SODA API with $limit/$offset (ordered by entityid
// for stable paging) and writes one compact NDJSON record per entity. Paging
// stops on an empty page or a short page (fewer than the page size), or when
// opts.MaxPages is reached.
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error) {
	if s == nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil Colorado entities source"),
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

	tempFile, err := os.CreateTemp(snapshotsDir, ".colorado_entities_*.ndjson.tmp")
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

	pageDelay := resolveDuration(opts.PageDelay, s.cfg.PageDelay, countryimport.DefaultPageDelay)
	requestTimeout := resolveDuration(opts.RequestTimeout, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(opts.UserAgent, s.cfg.UserAgent, countryimport.DefaultUserAgent)
	pageSize := s.cfg.PageSize
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	maxPages := opts.MaxPages

	hasher := sha256.New()
	writer := io.MultiWriter(tempFile, hasher)
	httpStatuses := make(map[string]int)

	var bytesDownloaded int64
	var recordsSeen int64
	var pagesDownloaded int
	offset := 0

	for pagesRequested := 0; maxPages <= 0 || pagesRequested < maxPages; pagesRequested++ {
		pageURL, err := buildPageURL(s.cfg.BaseURL, pageSize, offset)
		if err != nil {
			return countryimport.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindInvalidConfig,
				SourceSlug,
				s.cfg.BaseURL,
				"",
				0,
				errors.Wrap(err, "build Colorado page URL"),
			)
		}

		records, status, err := s.fetchPage(ctx, pageURL, userAgent, requestTimeout)
		if status != 0 {
			httpStatuses[strconv.Itoa(offset)] = status
		}
		if err != nil {
			return countryimport.DownloadResult{}, err
		}

		if len(records) == 0 {
			break
		}

		pagesDownloaded++
		for _, record := range records {
			line, err := compactRecordLine(record)
			if err != nil {
				return countryimport.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindRemoteDecode,
					SourceSlug,
					pageURL,
					"",
					0,
					errors.Wrap(err, "compact Colorado record line"),
				)
			}
			written, err := writer.Write(line)
			if err != nil {
				return countryimport.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO,
					SourceSlug,
					pageURL,
					tempPath,
					0,
					errors.Wrap(err, "write Colorado record line"),
				)
			}
			if written != len(line) {
				return countryimport.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO,
					SourceSlug,
					pageURL,
					tempPath,
					0,
					errors.Wrap(io.ErrShortWrite, "write Colorado record line"),
				)
			}
			bytesDownloaded += int64(written)
			recordsSeen++
		}

		if len(records) < pageSize {
			break
		}
		if maxPages > 0 && pagesRequested+1 >= maxPages {
			break
		}
		offset += pageSize
		if err := waitPageDelay(ctx, pageDelay); err != nil {
			return countryimport.DownloadResult{}, err
		}
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
		"colorado_entities_"+startedAt.Format("20060102T150405.000000000Z")+".ndjson",
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
		PagesDownloaded: pagesDownloaded,
		SHA256:          sha256Hex,
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		Duration:        finishedAt.Sub(startedAt),
	}
	metadata := countryimport.DownloadMetadata{
		SourceSlug:      result.SourceSlug,
		SourceName:      SourceName,
		BaseURL:         s.cfg.BaseURL,
		SnapshotPath:    result.SnapshotPath,
		StartedAt:       result.StartedAt,
		FinishedAt:      result.FinishedAt,
		DurationMS:      result.Duration.Milliseconds(),
		BytesDownloaded: result.BytesDownloaded,
		RecordsSeen:     result.RecordsSeen,
		PagesDownloaded: result.PagesDownloaded,
		FirstPage:       1,
		LastPage:        pagesDownloaded,
		SHA256:          result.SHA256,
		HTTPStatuses:    nilIfEmpty(httpStatuses),
		License:         "Open data (Socrata); verify Colorado open-data terms",
		Attribution:     "Colorado Secretary of State / Colorado Information Marketplace",
	}

	if err := s.saveDownloadMetadata(ctx, metadata); err != nil {
		return result, err
	}

	return result, nil
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
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			pageURL,
			"",
			0,
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
			countryimport.Classify(err),
			SourceSlug,
			pageURL,
			"",
			0,
			errors.Wrap(err, "get Colorado page"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return nil, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			pageURL,
			"",
			resp.StatusCode,
			errors.Newf("unexpected HTTP status %d", resp.StatusCode),
		)
	}

	var records []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return nil, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			pageURL,
			"",
			resp.StatusCode,
			errors.Wrap(err, "decode Colorado page"),
		)
	}

	return records, resp.StatusCode, nil
}

func buildPageURL(baseURL string, pageSize int, offset int) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	query.Set("$limit", strconv.Itoa(pageSize))
	query.Set("$offset", strconv.Itoa(offset))
	query.Set("$order", "entityid")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func compactRecordLine(record json.RawMessage) ([]byte, error) {
	line := bytes.Buffer{}
	if err := json.Compact(&line, record); err != nil {
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
			errors.Wrap(ctx.Err(), "wait between Colorado pages"),
		)
	case <-timer.C:
		return nil
	}
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
