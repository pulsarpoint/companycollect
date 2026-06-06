package irseobmf

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

// Download fetches the configured EO BMF CSV files and converts every data row
// into one NDJSON record, concatenating all files into a single snapshot. Files
// are streamed row by row so a multi-hundred-MB extract never loads fully into
// memory. opts.MaxPages caps the number of files (useful for a smoke run).
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error) {
	if s == nil {
		return countryimport.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil IRS EO BMF source"),
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

	tempFile, err := os.CreateTemp(snapshotsDir, ".irs_eo_bmf_*.ndjson.tmp")
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
	urls := s.cfg.DownloadURLs
	if len(urls) == 0 {
		urls = DefaultDownloadURLs
	}
	maxFiles := opts.MaxPages

	hasher := sha256.New()
	writer := io.MultiWriter(tempFile, hasher)
	httpStatuses := make(map[string]int)

	var bytesDownloaded int64
	var recordsSeen int64
	var skippedRows int64
	var filesDownloaded int

	emit := func(record IrsEoBmfRecord) error {
		line, err := json.Marshal(record)
		if err != nil {
			return countryimport.WrapSourceError(
				countryimport.ErrorKindRemoteDecode,
				SourceSlug,
				"",
				tempPath,
				0,
				errors.Wrap(err, "marshal EO BMF record"),
			)
		}
		line = append(line, '\n')
		written, err := writer.Write(line)
		if err != nil {
			return countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO,
				SourceSlug,
				"",
				tempPath,
				0,
				errors.Wrap(err, "write EO BMF line"),
			)
		}
		if written != len(line) {
			return countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO,
				SourceSlug,
				"",
				tempPath,
				0,
				errors.Wrap(io.ErrShortWrite, "write EO BMF line"),
			)
		}
		bytesDownloaded += int64(written)
		recordsSeen++
		return nil
	}

	for i, fileURL := range urls {
		if maxFiles > 0 && i >= maxFiles {
			break
		}
		if i > 0 {
			if err := waitPageDelay(ctx, pageDelay); err != nil {
				return countryimport.DownloadResult{}, err
			}
		}

		skipped, status, err := s.streamCSVFile(ctx, fileURL, userAgent, requestTimeout, emit)
		if status != 0 {
			httpStatuses[fileLabel(fileURL)] = status
		}
		if err != nil {
			return countryimport.DownloadResult{}, err
		}
		skippedRows += skipped
		filesDownloaded++
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

	if skippedRows > 0 {
		slog.WarnContext(ctx, "skipped malformed EO BMF rows",
			"source", SourceSlug,
			"skipped_rows", skippedRows,
		)
	}

	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(
		snapshotsDir,
		"irs_eo_bmf_"+startedAt.Format("20060102T150405.000000000Z")+".ndjson",
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
		PagesDownloaded: filesDownloaded,
		SHA256:          sha256Hex,
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		Duration:        finishedAt.Sub(startedAt),
	}
	metadata := countryimport.DownloadMetadata{
		SourceSlug:      result.SourceSlug,
		SourceName:      SourceName,
		BaseURL:         firstURL(urls),
		SnapshotPath:    result.SnapshotPath,
		StartedAt:       result.StartedAt,
		FinishedAt:      result.FinishedAt,
		DurationMS:      result.Duration.Milliseconds(),
		BytesDownloaded: result.BytesDownloaded,
		RecordsSeen:     result.RecordsSeen,
		PagesDownloaded: result.PagesDownloaded,
		FirstPage:       1,
		LastPage:        filesDownloaded,
		SHA256:          result.SHA256,
		HTTPStatuses:    nilIfEmpty(httpStatuses),
		License:         "U.S. Government work / public domain",
		Attribution:     "U.S. Internal Revenue Service (IRS) Exempt Organizations Business Master File",
	}

	if err := s.saveDownloadMetadata(ctx, metadata); err != nil {
		return result, err
	}

	return result, nil
}

// streamCSVFile fetches one CSV file and emits each well-formed data row. Rows
// whose field count does not match the header, and rows the CSV reader cannot
// parse, are logged and skipped rather than aborting the whole download.
func (s *Source) streamCSVFile(ctx context.Context, fileURL string, userAgent string, requestTimeout time.Duration, emit func(IrsEoBmfRecord) error) (int64, int, error) {
	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, fileURL, nil)
	if err != nil {
		return 0, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig,
			SourceSlug,
			fileURL,
			"",
			0,
			errors.Wrap(err, "create EO BMF request"),
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
		return 0, 0, countryimport.WrapSourceError(
			countryimport.Classify(err),
			SourceSlug,
			fileURL,
			"",
			0,
			errors.Wrap(err, "get EO BMF file"),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return 0, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus,
			SourceSlug,
			fileURL,
			"",
			resp.StatusCode,
			errors.Newf("unexpected HTTP status %d", resp.StatusCode),
		)
	}

	skipped, err := streamCSV(ctx, resp.Body, fileURL, emit)
	if err != nil {
		return skipped, resp.StatusCode, err
	}
	return skipped, resp.StatusCode, nil
}

func streamCSV(ctx context.Context, body io.Reader, fileURL string, emit func(IrsEoBmfRecord) error) (int64, error) {
	reader := csv.NewReader(body)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.ReuseRecord = true

	var header []string
	var index map[string]int
	var skipped int64
	var rowNumber int64

	for {
		if err := ctx.Err(); err != nil {
			return skipped, countryimport.WrapSourceError(
				countryimport.Classify(err),
				SourceSlug,
				fileURL,
				"",
				0,
				errors.Wrap(err, "stream EO BMF csv"),
			)
		}

		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			skipped++
			slog.WarnContext(ctx, "parse EO BMF csv row",
				"source", SourceSlug,
				"url", fileURL,
				"error", readErr,
			)
			continue
		}

		if header == nil {
			header = append([]string(nil), row...)
			index = buildColumnIndex(header)
			continue
		}

		rowNumber++
		if len(row) != len(header) {
			skipped++
			slog.WarnContext(ctx, "skip malformed EO BMF csv row",
				"source", SourceSlug,
				"url", fileURL,
				"row", rowNumber,
				"fields", len(row),
				"want", len(header),
			)
			continue
		}

		if err := emit(recordFromRow(row, index)); err != nil {
			return skipped, err
		}
	}

	return skipped, nil
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
			errors.Wrap(ctx.Err(), "wait between EO BMF files"),
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

func fileLabel(fileURL string) string {
	if base := path.Base(fileURL); base != "" && base != "." && base != "/" {
		return base
	}
	return fileURL
}

func firstURL(urls []string) string {
	if len(urls) == 0 {
		return ""
	}
	return urls[0]
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
