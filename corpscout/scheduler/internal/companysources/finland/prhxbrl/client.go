package prhxbrl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

type DiscoveryPage struct {
	TotalResults int64                 `json:"totalResults"`
	Financials   []DiscoveredStatement `json:"financials"`
}

type DiscoveredStatement struct {
	BusinessID       string `json:"businessId"`
	FinancialDate    string `json:"financialDate"`
	RegistrationDate string `json:"registrationDate"`
}

func buildAllFinancialStatementsURL(baseURL string, registeredDateStart string, registeredDateEnd string, page int) (string, error) {
	parsed, err := parsePRHXBRLBaseURL(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = joinPRHXBRLPath(parsed.Path, "all_financial_statements")
	query := parsed.Query()
	query.Set("registeredDateStart", registeredDateStart)
	query.Set("registeredDateEnd", registeredDateEnd)
	query.Set("page", strconv.Itoa(page))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func buildFinancialStatementURL(baseURL string, businessID string, financialDate string) (string, error) {
	parsed, err := parsePRHXBRLBaseURL(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = joinPRHXBRLPath(parsed.Path, "financial")
	query := parsed.Query()
	query.Set("businessId", businessID)
	query.Set("financialDate", financialDate)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func parsePRHXBRLBaseURL(baseURL string) (*url.URL, error) {
	if baseURL == "" {
		return nil, errors.New("PRH XBRL base url is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrap(err, "parse PRH XBRL base url")
	}
	return parsed, nil
}

func joinPRHXBRLPath(basePath string, endpoint string) string {
	if basePath == "" || basePath == "/" {
		return "/" + endpoint
	}
	return path.Join(basePath, endpoint)
}

func downloadDiscoveryPage(ctx context.Context, client *http.Client, baseURL string, registeredDateStart string, registeredDateEnd string, page int, userAgentRequired bool) (DiscoveryPage, error) {
	if client == nil {
		client = http.DefaultClient
	}
	pageURL, err := buildAllFinancialStatementsURL(baseURL, registeredDateStart, registeredDateEnd, page)
	if err != nil {
		return DiscoveryPage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return DiscoveryPage{}, errors.Wrap(err, "create PRH XBRL discovery request")
	}
	if userAgentRequired {
		req.Header.Set("User-Agent", companysources.DownloadUserAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return DiscoveryPage{}, errors.Wrap(err, "download PRH XBRL discovery page")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return DiscoveryPage{}, errors.Errorf("download PRH XBRL discovery page: status %d", resp.StatusCode)
	}

	var pageResult DiscoveryPage
	if err := json.NewDecoder(resp.Body).Decode(&pageResult); err != nil {
		return DiscoveryPage{}, errors.Wrap(err, "decode PRH XBRL discovery page")
	}
	return pageResult, nil
}
