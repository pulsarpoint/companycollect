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
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

func (s *Source) Download(ctx context.Context, opts sourcespec.DownloadOptions) (sourcespec.DownloadResult, error) {
	if s == nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil SEC EDGAR source"),
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
			errors.Wrap(err, "create SEC EDGAR run directory"),
		)
	}

	downloadURL := resolveString(s.cfg.DownloadURL, DefaultDownloadURL)
	requestTimeout := resolveDuration(0, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(s.cfg.UserAgent, DefaultUserAgent)

	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			downloadURL,
			"",
			resp.StatusCode,
			errors.Newf("unexpected SEC EDGAR HTTP status %d", resp.StatusCode),
		)
	}

	tempFile, err := os.CreateTemp(runDir, ".source.json.*.tmp")
	if err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			runDir,
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			downloadURL,
			tempPath,
			resp.StatusCode,
			errors.Wrap(err, "decode SEC EDGAR temporary snapshot"),
		)
	}

	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(runDir, "source.json")
	if err := os.Rename(tempPath, finalPath); err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
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
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			downloadURL,
			finalPath,
			0,
			errors.Wrap(err, "stat SEC EDGAR snapshot"),
		)
	}

	_ = bytesDownloaded
	_ = sha256Hex
	return sourcespec.DownloadResult{RunDir: runDir, SourcePath: finalPath, RecordsSeen: int64(len(records))}, nil
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
