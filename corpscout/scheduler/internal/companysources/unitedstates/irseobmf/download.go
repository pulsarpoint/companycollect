package irseobmf

import (
	"bufio"
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
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

var defaultDownloadFiles = []string{"eo1.csv", "eo2.csv", "eo3.csv", "eo4.csv"}

var csvColumns = []string{
	"EIN", "NAME", "ICO", "STREET", "CITY", "STATE", "ZIP", "GROUP",
	"SUBSECTION", "AFFILIATION", "CLASSIFICATION", "RULING", "DEDUCTIBILITY",
	"FOUNDATION", "ACTIVITY", "ORGANIZATION", "STATUS", "TAX_PERIOD",
	"ASSET_CD", "INCOME_CD", "FILING_REQ_CD", "PF_FILING_REQ_CD", "ACCT_PD",
	"ASSET_AMT", "INCOME_AMT", "REVENUE_AMT", "NTEE_CD", "SORT_NAME",
}

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	if opts.FileKind != "" && opts.FileKind != "source_snapshot" {
		return companysources.DownloadedFile{}, errors.Errorf("unsupported IRS EO BMF file kind %q", opts.FileKind)
	}
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.ndjson"
	}
	path, err := companysources.SafeRunRelativePath(opts.RunDir, relativePath)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}

	written, err := downloadEOFilesAsNDJSON(ctx, http.DefaultClient, opts.SourceURL, opts.UserAgentRequired, opts.Config, path)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       relativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}

func downloadEOFilesAsNDJSON(ctx context.Context, client *http.Client, sourceURL string, userAgentRequired bool, config map[string]any, outputPath string) (companysources.FileWriteResult, error) {
	urls, err := eoDownloadURLs(sourceURL, config)
	if err != nil {
		return companysources.FileWriteResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "create IRS EO BMF snapshot directory")
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "create IRS EO BMF snapshot")
	}
	defer file.Close()

	hasher := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hasher))
	var bytesWritten int64
	var recordsWritten int64
	var skippedRows int64

	emit := func(record IrsEoBmfRecord) error {
		line, err := json.Marshal(record)
		if err != nil {
			return errors.Wrap(err, "marshal IRS EO BMF record")
		}
		n, err := writer.Write(line)
		if err != nil {
			return errors.Wrap(err, "write IRS EO BMF record")
		}
		bytesWritten += int64(n)
		n, err = writer.WriteString("\n")
		if err != nil {
			return errors.Wrap(err, "write IRS EO BMF newline")
		}
		bytesWritten += int64(n)
		recordsWritten++
		return nil
	}

	for _, fileURL := range urls {
		skipped, err := downloadEOFile(ctx, client, fileURL, userAgentRequired, emit)
		if err != nil {
			return companysources.FileWriteResult{}, err
		}
		skippedRows += skipped
	}
	if skippedRows > 0 {
		slog.WarnContext(ctx, "skipped malformed IRS EO BMF rows", "skipped_rows", skippedRows)
	}
	if err := writer.Flush(); err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "flush IRS EO BMF snapshot")
	}
	return companysources.FileWriteResult{
		SourceFilePath:     outputPath,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: bytesWritten,
		RecordsWritten:     recordsWritten,
	}, nil
}

func eoDownloadURLs(sourceURL string, config map[string]any) ([]string, error) {
	if configured, ok := stringSliceConfig(config, "download_urls"); ok {
		return configured, nil
	}
	if strings.TrimSpace(sourceURL) == "" {
		return nil, errors.New("source url is required")
	}
	urls := make([]string, 0, len(defaultDownloadFiles))
	for _, filename := range defaultDownloadFiles {
		fileURL, err := joinURLPath(sourceURL, filename)
		if err != nil {
			return nil, err
		}
		urls = append(urls, fileURL)
	}
	return urls, nil
}

func downloadEOFile(ctx context.Context, client *http.Client, fileURL string, userAgentRequired bool, emit func(IrsEoBmfRecord) error) (int64, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return 0, errors.Wrap(err, "create IRS EO BMF request")
	}
	if userAgentRequired {
		req.Header.Set("User-Agent", companysources.DownloadUserAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.Wrap(err, "download IRS EO BMF file")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0, errors.Errorf("download IRS EO BMF file: status %d", resp.StatusCode)
	}
	return streamEOCSV(ctx, resp.Body, fileURL, emit)
}

func streamEOCSV(ctx context.Context, body io.Reader, fileURL string, emit func(IrsEoBmfRecord) error) (int64, error) {
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
			return skipped, errors.Wrap(err, "stream IRS EO BMF csv")
		}
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			return skipped, nil
		}
		if readErr != nil {
			skipped++
			slog.WarnContext(ctx, "parse IRS EO BMF csv row", "url", fileURL, "error", readErr)
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
			slog.WarnContext(ctx, "skip malformed IRS EO BMF csv row", "url", fileURL, "row", rowNumber, "fields", len(row), "want", len(header))
			continue
		}
		if err := emit(recordFromRow(row, index)); err != nil {
			return skipped, err
		}
	}
}

func recordFromRow(row []string, index map[string]int) IrsEoBmfRecord {
	get := func(name string) string {
		if i, ok := index[name]; ok && i < len(row) {
			return row[i]
		}
		return ""
	}
	return IrsEoBmfRecord{
		EIN:            get("EIN"),
		Name:           get("NAME"),
		InCareOf:       get("ICO"),
		Street:         get("STREET"),
		City:           get("CITY"),
		State:          get("STATE"),
		Zip:            get("ZIP"),
		Group:          get("GROUP"),
		Subsection:     get("SUBSECTION"),
		Affiliation:    get("AFFILIATION"),
		Classification: get("CLASSIFICATION"),
		Ruling:         get("RULING"),
		Deductibility:  get("DEDUCTIBILITY"),
		Foundation:     get("FOUNDATION"),
		Activity:       get("ACTIVITY"),
		Organization:   get("ORGANIZATION"),
		Status:         get("STATUS"),
		TaxPeriod:      get("TAX_PERIOD"),
		AssetCD:        get("ASSET_CD"),
		IncomeCD:       get("INCOME_CD"),
		FilingReqCD:    get("FILING_REQ_CD"),
		PFFilingReqCD:  get("PF_FILING_REQ_CD"),
		AcctPD:         get("ACCT_PD"),
		AssetAmt:       get("ASSET_AMT"),
		IncomeAmt:      get("INCOME_AMT"),
		RevenueAmt:     get("REVENUE_AMT"),
		NTEECD:         get("NTEE_CD"),
		SortName:       get("SORT_NAME"),
	}
}

func buildColumnIndex(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[strings.TrimSpace(name)] = i
	}
	return index
}

func joinURLPath(baseURL string, filename string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", errors.Wrap(err, "parse IRS EO BMF source url")
	}
	parsed.Path = path.Join(parsed.Path, filename)
	return parsed.String(), nil
}

func stringSliceConfig(config map[string]any, key string) ([]string, bool) {
	raw, ok := config[key]
	if !ok {
		return nil, false
	}
	switch typed := raw.(type) {
	case []string:
		values := nonEmptyStrings(typed)
		return values, len(values) > 0
	case []any:
		values := make([]string, 0, len(typed))
		for _, value := range typed {
			if stringValue, ok := value.(string); ok {
				values = append(values, stringValue)
			}
		}
		values = nonEmptyStrings(values)
		return values, len(values) > 0
	default:
		return nil, false
	}
}

func nonEmptyStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
