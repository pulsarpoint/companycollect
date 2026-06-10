package prhytj

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const defaultCompaniesPageSize = 100

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	switch opts.FileKind {
	case "", "source_snapshot":
		return downloadSourceSnapshot(ctx, opts)
	case "code_list":
		return downloadCodeList(ctx, opts)
	default:
		return companysources.DownloadedFile{}, errors.Errorf("unsupported PRH YTJ file kind %q", opts.FileKind)
	}
}

func downloadSourceSnapshot(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.ndjson"
	}
	path, err := companysources.SafeRunRelativePath(opts.RunDir, relativePath)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	sourceURL := opts.SourceURL
	if sourceURL == "" {
		sourceURL = DefaultBaseURL
	}

	companies, err := downloadAllCompanyPages(ctx, sourceURL, opts.UserAgentRequired)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}

	records := make([]json.RawMessage, 0, len(companies))
	for _, company := range companies {
		raw, err := json.Marshal(company)
		if err != nil {
			return companysources.DownloadedFile{}, errors.Wrap(err, "encode PRH YTJ company")
		}
		records = append(records, raw)
	}
	written, err := companysources.WriteRawMessagesAsNDJSON(path, records)
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

func downloadAllCompanyPages(ctx context.Context, sourceURL string, userAgentRequired bool) ([]CompanyRecord, error) {
	var companies []CompanyRecord
	var totalResults *int64
	for pageNumber := 1; ; pageNumber++ {
		pageURL, err := companyPageURL(sourceURL, pageNumber)
		if err != nil {
			return nil, err
		}
		page, err := downloadPage(ctx, http.DefaultClient, pageURL, userAgentRequired)
		if err != nil {
			return nil, err
		}
		if page.TotalResults != nil {
			totalResults = page.TotalResults
		}
		companies = append(companies, page.Companies...)
		if totalResults != nil && int64(len(companies)) >= *totalResults {
			return companies, nil
		}
		if len(page.Companies) == 0 {
			return companies, nil
		}
		if totalResults == nil && len(page.Companies) < defaultCompaniesPageSize {
			return companies, nil
		}
	}
}

func companyPageURL(sourceURL string, pageNumber int) (string, error) {
	parsed, err := url.Parse(sourceURL)
	if err != nil {
		return "", errors.Wrap(err, "parse PRH YTJ companies url")
	}
	query := parsed.Query()
	query.Set("page", strconv.Itoa(pageNumber))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func downloadCodeList(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	relativePath := opts.RelativePath
	if relativePath == "" {
		return companysources.DownloadedFile{}, errors.New("relative path is required")
	}
	code, _ := opts.Config["code"].(string)
	if code == "" {
		return companysources.DownloadedFile{}, errors.New("PRH YTJ code list code is required")
	}
	lang, _ := opts.Config["lang"].(string)
	if lang == "" {
		lang = "en"
	}
	sourceURL := opts.SourceURL
	if sourceURL == "" {
		sourceURL = DefaultBaseURL
	}
	descriptionURL, err := prhDescriptionURL(sourceURL, code, lang)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}

	written, err := companysources.DownloadDirectFile(ctx, http.DefaultClient, companysources.DirectFileDownload{
		URL:               descriptionURL,
		RunDir:            opts.RunDir,
		RelativePath:      relativePath,
		UserAgentRequired: opts.UserAgentRequired,
	})
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

func prhDescriptionURL(sourceURL string, code string, lang string) (string, error) {
	parsed, err := url.Parse(sourceURL)
	if err != nil {
		return "", errors.Wrap(err, "parse PRH YTJ description url")
	}
	parsed.Path = "/opendata-ytj-api/v3/description"
	query := parsed.Query()
	query.Set("code", code)
	query.Set("lang", lang)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func downloadPage(ctx context.Context, client *http.Client, sourceURL string, userAgentRequired bool) (Page, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if sourceURL == "" {
		return Page{}, errors.New("source url is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return Page{}, errors.Wrap(err, "create PRH YTJ download request")
	}
	if userAgentRequired {
		req.Header.Set("User-Agent", companysources.DownloadUserAgent)
	}

	resp, err := client.Do(req)
	if err != nil {
		return Page{}, errors.Wrap(err, "download PRH YTJ page")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Page{}, errors.Errorf("download PRH YTJ page: status %d", resp.StatusCode)
	}

	var page Page
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return Page{}, errors.Wrap(err, "decode PRH YTJ page")
	}
	return page, nil
}
