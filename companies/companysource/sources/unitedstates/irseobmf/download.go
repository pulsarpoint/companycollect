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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

// Download fetches the configured EO BMF CSV files, converts each row to one
// compact NDJSON record keyed by column name, and writes a single NDJSON
// snapshot. Each CSV file is treated as a "page": opts.MaxPages bounds the
// number of files fetched for smoke tests.
func (s *Source) Download(ctx context.Context, opts sourcespec.DownloadOptions) (sourcespec.DownloadResult, error) {
	if s == nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState, SourceSlug, "", "", 0,
			errors.New("nil IRS EO BMF source"),
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
			errors.Wrap(err, "create IRS EO BMF run directory"),
		)
	}

	files := s.cfg.Files
	if len(files) == 0 {
		files = DefaultFiles
	}
	if opts.MaxPages > 0 && opts.MaxPages < len(files) {
		files = files[:opts.MaxPages]
	}

	requestTimeout := resolveDuration(0, s.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	userAgent := resolveString(s.cfg.UserAgent, countryimport.DefaultUserAgent)
	pageDelay := time.Duration(0)

	tempFile, err := os.CreateTemp(runDir, ".source.ndjson.*.tmp")
	if err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", runDir, 0,
			errors.Wrap(err, "create IRS EO BMF temporary snapshot"),
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
	httpStatuses := make(map[string]int)

	var bytesWritten int64
	var recordsSeen int64
	var decodeErrors int64
	var pagesDownloaded int

	for i, file := range files {
		if i > 0 && pageDelay > 0 {
			if err := waitDelay(ctx, pageDelay); err != nil {
				return sourcespec.DownloadResult{}, err
			}
		}
		fileURL, err := buildFileURL(s.cfg.BaseURL, file)
		if err != nil {
			return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
				countryimport.ErrorKindInvalidConfig, SourceSlug, s.cfg.BaseURL, "", 0,
				errors.Wrapf(err, "build IRS EO BMF file URL for %s", file),
			)
		}

		written, seen, decoded, status, err := s.downloadFile(ctx, fileURL, file, userAgent, requestTimeout, writer)
		if status != 0 {
			httpStatuses[file] = status
		}
		if err != nil {
			return sourcespec.DownloadResult{}, err
		}
		bytesWritten += written
		recordsSeen += seen
		decodeErrors += decoded
		pagesDownloaded++
	}

	if err := tempFile.Close(); err != nil {
		tempClosed = true
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", tempPath, 0,
			errors.Wrap(err, "close IRS EO BMF temporary snapshot"),
		)
	}
	tempClosed = true

	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(runDir, "source.ndjson")
	if err := os.Rename(tempPath, finalPath); err != nil {
		return sourcespec.DownloadResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", finalPath, 0,
			errors.Wrap(err, "rename IRS EO BMF snapshot"),
		)
	}
	keepTemp = true

	if decodeErrors > 0 {
		slog.WarnContext(ctx, "IRS EO BMF download skipped malformed rows",
			"source", SourceSlug, "decode_errors", decodeErrors)
	}

	_ = bytesWritten
	_ = pagesDownloaded
	_ = sha256Hex
	_ = httpStatuses
	return sourcespec.DownloadResult{RunDir: runDir, SourcePath: finalPath, RecordsSeen: recordsSeen}, nil
}

// downloadFile streams one CSV file, validates its header, converts each row to
// an NDJSON line, and writes those lines through writer. It returns the bytes
// written, records seen, and decode errors for the file.
func (s *Source) downloadFile(ctx context.Context, fileURL string, file string, userAgent string, requestTimeout time.Duration, writer io.Writer) (int64, int64, int64, int, error) {
	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, fileURL, nil)
	if err != nil {
		return 0, 0, 0, 0, countryimport.WrapSourceError(
			countryimport.ErrorKindInvalidConfig, SourceSlug, fileURL, "", 0,
			errors.Wrap(err, "create IRS EO BMF file request"),
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
		return 0, 0, 0, 0, countryimport.WrapSourceError(
			countryimport.Classify(err), SourceSlug, fileURL, "", 0,
			errors.Wrapf(err, "download IRS EO BMF file %s", file),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return 0, 0, 0, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindHTTPStatus, SourceSlug, fileURL, "", resp.StatusCode,
			errors.Newf("unexpected IRS EO BMF HTTP status %d for %s", resp.StatusCode, file),
		)
	}

	reader := csv.NewReader(resp.Body)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		return 0, 0, 0, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode, SourceSlug, fileURL, "", resp.StatusCode,
			errors.Wrapf(err, "read IRS EO BMF header for %s", file),
		)
	}
	if err := validateHeader(header); err != nil {
		return 0, 0, 0, resp.StatusCode, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode, SourceSlug, fileURL, "", resp.StatusCode,
			errors.Wrapf(err, "validate IRS EO BMF header for %s", file),
		)
	}

	var bytesWritten int64
	var recordsSeen int64
	var decodeErrors int64
	var rowNumber int64
	for {
		if err := ctx.Err(); err != nil {
			return bytesWritten, recordsSeen, decodeErrors, resp.StatusCode, countryimport.WrapSourceError(
				countryimport.Classify(err), SourceSlug, fileURL, "", 0,
				errors.Wrap(err, "download IRS EO BMF file"),
			)
		}
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		rowNumber++
		if err != nil {
			decodeErrors++
			slog.WarnContext(ctx, "read IRS EO BMF CSV row",
				"source", SourceSlug, "file", file, "row", rowNumber, "error", err)
			continue
		}

		record, err := rowToRecord(row)
		if err != nil {
			decodeErrors++
			slog.WarnContext(ctx, "convert IRS EO BMF CSV row",
				"source", SourceSlug, "file", file, "row", rowNumber, "error", err)
			continue
		}

		line, err := json.Marshal(record)
		if err != nil {
			decodeErrors++
			slog.WarnContext(ctx, "marshal IRS EO BMF record",
				"source", SourceSlug, "file", file, "row", rowNumber, "error", err)
			continue
		}
		line = append(line, '\n')
		written, err := writer.Write(line)
		if err != nil {
			return bytesWritten, recordsSeen, decodeErrors, resp.StatusCode, countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO, SourceSlug, fileURL, "", resp.StatusCode,
				errors.Wrap(err, "write IRS EO BMF line"),
			)
		}
		if written != len(line) {
			return bytesWritten, recordsSeen, decodeErrors, resp.StatusCode, countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO, SourceSlug, fileURL, "", resp.StatusCode,
				errors.Wrap(io.ErrShortWrite, "write IRS EO BMF line"),
			)
		}
		bytesWritten += int64(written)
		recordsSeen++
	}

	return bytesWritten, recordsSeen, decodeErrors, resp.StatusCode, nil
}

func buildFileURL(baseURL string, file string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}
	ref, err := url.Parse(strings.TrimSpace(file))
	if err != nil {
		return "", err
	}
	return base.ResolveReference(ref).String(), nil
}

func waitDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return countryimport.WrapSourceError(
			countryimport.Classify(ctx.Err()), SourceSlug, "", "", 0,
			errors.Wrap(ctx.Err(), "wait between IRS EO BMF files"),
		)
	case <-timer.C:
		return nil
	}
}
