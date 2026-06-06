package samgoventity

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

type downloadEnvelope struct {
	TotalRecords *int64            `json:"totalRecords"`
	EntityData   []json.RawMessage `json:"entityData"`
}

// Download pages the SAM.gov Entity API with page/size, authenticating via the
// X-Api-Key header (so the key never appears in a URL, error, or metadata), and
// writes one compact NDJSON record per entity. Paging stops on an empty/short
// page, when totalRecords is reached, or at opts.MaxPages.
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error) {
	if s == nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil SAM.gov entity source"),
		)
	}
	if strings.TrimSpace(s.cfg.APIKey) == "" {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			s.cfg.BaseURL,
			"",
			0,
			errors.New("missing SAM.gov API key (set SAM_GOV_ENTITY_API_KEY)"),
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

	tempFile, err := os.CreateTemp(snapshotsDir, ".sam_gov_entity_*.ndjson.tmp")
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
	var totalResultsReported *int64
	page := 0

	for pagesRequested := 0; maxPages <= 0 || pagesRequested < maxPages; pagesRequested++ {
		displayURL, err := buildPageURL(s.cfg.BaseURL, s.cfg.SamRegistered, page, pageSize)
		if err != nil {
			return countryimport.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindInvalidConfig,
				SourceSlug,
				s.cfg.BaseURL,
				"",
				0,
				errors.Wrap(err, "build SAM page URL"),
			)
		}

		envelope, status, err := s.fetchPage(ctx, displayURL, userAgent, requestTimeout)
		if status != 0 {
			httpStatuses[strconv.Itoa(page)] = status
		}
		if err != nil {
			return countryimport.DownloadResult{}, err
		}

		if envelope.TotalRecords != nil && totalResultsReported == nil {
			total := *envelope.TotalRecords
			totalResultsReported = &total
		}
		if len(envelope.EntityData) == 0 {
			break
		}

		pagesDownloaded++
		for _, entity := range envelope.EntityData {
			line, err := compactEntityLine(entity)
			if err != nil {
				return countryimport.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindRemoteDecode,
					SourceSlug,
					displayURL,
					"",
					0,
					errors.Wrap(err, "compact SAM entity line"),
				)
			}
			written, err := writer.Write(line)
			if err != nil {
				return countryimport.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO,
					SourceSlug,
					displayURL,
					tempPath,
					0,
					errors.Wrap(err, "write SAM entity line"),
				)
			}
			if written != len(line) {
				return countryimport.DownloadResult{}, countryimport.WrapSourceError(
					countryimport.ErrorKindFileIO,
					SourceSlug,
					displayURL,
					tempPath,
					0,
					errors.Wrap(io.ErrShortWrite, "write SAM entity line"),
				)
			}
			bytesDownloaded += int64(written)
			recordsSeen++
		}

		if len(envelope.EntityData) < pageSize {
			break
		}
		if totalResultsReported != nil && *totalResultsReported > 0 && recordsSeen >= *totalResultsReported {
			break
		}
		if maxPages > 0 && pagesRequested+1 >= maxPages {
			break
		}
		page++
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
		"sam_gov_entity_"+startedAt.Format("20060102T150405.000000000Z")+".ndjson",
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
		SourceSlug:           result.SourceSlug,
		SourceName:           SourceName,
		BaseURL:              s.cfg.BaseURL,
		SnapshotPath:         result.SnapshotPath,
		StartedAt:            result.StartedAt,
		FinishedAt:           result.FinishedAt,
		DurationMS:           result.Duration.Milliseconds(),
		BytesDownloaded:      result.BytesDownloaded,
		RecordsSeen:          result.RecordsSeen,
		PagesDownloaded:      result.PagesDownloaded,
		FirstPage:            0,
		LastPage:             page,
		TotalResultsReported: totalResultsReported,
		SHA256:               result.SHA256,
		HTTPStatuses:         nilIfEmpty(httpStatuses),
		License:              "Public extract released under FOIA (Public sensitivity level only)",
		Attribution:          "U.S. General Services Administration (GSA) — SAM.gov",
	}

	if err := s.saveDownloadMetadata(ctx, metadata); err != nil {
		return result, err
	}

	return result, nil
}

// fetchPage requests one page. The api key is sent via the X-Api-Key header and
// never placed in the URL, so displayURL (and any error wrapping it) is safe to
// log.
func (s *Source) fetchPage(ctx context.Context, displayURL string, userAgent string, requestTimeout time.Duration) (downloadEnvelope, int, error) {
	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, displayURL, nil)
	if err != nil {
		return downloadEnvelope{}, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			displayURL,
			"",
			0,
			errors.Wrap(err, "create SAM page request"),
		)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Api-Key", s.cfg.APIKey)

	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return downloadEnvelope{}, 0, countryimport.WrapSourceError(
			countryimport.Classify(err),
			SourceSlug,
			displayURL,
			"",
			0,
			errors.Wrap(err, "get SAM page"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return downloadEnvelope{}, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			displayURL,
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
			displayURL,
			"",
			resp.StatusCode,
			errors.Wrap(err, "decode SAM page"),
		)
	}

	return envelope, resp.StatusCode, nil
}

// buildPageURL builds the request URL WITHOUT the api key (auth is via header).
func buildPageURL(baseURL string, samRegistered string, page int, size int) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	if trimmed := strings.TrimSpace(samRegistered); trimmed != "" {
		query.Set("samRegistered", trimmed)
	}
	query.Set("page", strconv.Itoa(page))
	query.Set("size", strconv.Itoa(size))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func compactEntityLine(entity json.RawMessage) ([]byte, error) {
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
			countryimport.Classify(ctx.Err()),
			SourceSlug,
			"",
			"",
			0,
			errors.Wrap(ctx.Err(), "wait between SAM pages"),
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
