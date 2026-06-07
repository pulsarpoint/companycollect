package secedgar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

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
			errors.Wrap(err, "create SEC EDGAR snapshots directory"),
		)
	}

	downloadURL := resolveString(s.cfg.DownloadURL, DefaultDownloadURL)
	requestTimeout := resolveDuration(opts.RequestTimeout, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(opts.UserAgent, s.cfg.UserAgent, countryimport.DefaultUserAgent)

	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			downloadURL,
			"",
			0,
			errors.Wrap(err, "create SEC EDGAR download request"),
		)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}

	client := s.httpClient
	if client == nil {
		client = &http.Client{}
	}
	resp, err := client.Do(req)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.Classify(err),
			SourceSlug,
			downloadURL,
			"",
			0,
			errors.Wrap(err, "download SEC EDGAR company tickers"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			downloadURL,
			"",
			resp.StatusCode,
			errors.Newf("unexpected SEC EDGAR HTTP status %d", resp.StatusCode),
		)
	}

	tempFile, err := os.CreateTemp(snapshotsDir, ".sec_edgar_company_tickers_*.json.tmp")
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			snapshotsDir,
			0,
			errors.Wrap(err, "create SEC EDGAR temporary snapshot"),
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
	bytesDownloaded, err := io.Copy(io.MultiWriter(tempFile, hasher), resp.Body)
	if err != nil {
		kind := countryimport.Classify(err)
		if kind == countryimport.ErrorKindUnknown {
			kind = countryimport.ErrorKindFileIO
		}
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			kind,
			SourceSlug,
			downloadURL,
			tempPath,
			resp.StatusCode,
			errors.Wrap(err, "write SEC EDGAR snapshot"),
		)
	}

	if err := tempFile.Close(); err != nil {
		tempClosed = true
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			tempPath,
			resp.StatusCode,
			errors.Wrap(err, "close SEC EDGAR temporary snapshot"),
		)
	}
	tempClosed = true

	payload, err := os.ReadFile(tempPath)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			tempPath,
			resp.StatusCode,
			errors.Wrap(err, "read SEC EDGAR temporary snapshot"),
		)
	}
	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			downloadURL,
			tempPath,
			resp.StatusCode,
			errors.Wrap(err, "decode SEC EDGAR temporary snapshot"),
		)
	}

	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(
		snapshotsDir,
		"sec_edgar_company_tickers_"+startedAt.Format("20060102T150405.000000000Z")+".json",
	)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			finalPath,
			0,
			errors.Wrap(err, "rename SEC EDGAR snapshot"),
		)
	}
	keepTemp = true

	if _, err := os.Stat(finalPath); err != nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			finalPath,
			0,
			errors.Wrap(err, "stat SEC EDGAR snapshot"),
		)
	}

	finishedAt := time.Now().UTC()
	result := countryimport.DownloadResult{
		SourceSlug:      SourceSlug,
		SnapshotPath:    finalPath,
		BytesDownloaded: bytesDownloaded,
		RecordsSeen:     int64(len(records)),
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
		SHA256:          result.SHA256,
		HTTPStatuses: map[string]int{
			"download": resp.StatusCode,
		},
		Attribution: "U.S. Securities and Exchange Commission (SEC) EDGAR",
	}

	if err := s.saveDownloadMetadata(ctx, metadata); err != nil {
		return result, err
	}

	return result, nil
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
			errors.Wrap(err, "save SEC EDGAR download metadata"),
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

func resolveString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
