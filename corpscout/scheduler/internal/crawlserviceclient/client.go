package crawlserviceclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

var ErrUnexpectedStatus = errors.New("crawl service returned unexpected status")

const errorBodyLimit = 4096

type DomainDiscoverLimits struct {
	MaxSearchCandidates      int `json:"max_search_candidates,omitempty"`
	MaxSiteChecks            int `json:"max_site_checks,omitempty"`
	SearchCandidateThreshold int `json:"search_candidate_threshold,omitempty"`
	DomainThreshold          int `json:"domain_threshold,omitempty"`
	TimeoutSeconds           int `json:"timeout_seconds,omitempty"`
}

type BrregDomainDiscoveryRequest struct {
	RecordID           string               `json:"record_id"`
	OrganizationNumber string               `json:"organization_number"`
	OrganizationName   *string              `json:"organization_name,omitempty"`
	RawPayload         json.RawMessage      `json:"raw_payload"`
	ExistingWebsite    *string              `json:"existing_website,omitempty"`
	Country            string               `json:"country,omitempty"`
	SearchProvider     string               `json:"search_provider,omitempty"`
	PromptVersion      string               `json:"prompt_version,omitempty"`
	Limits             DomainDiscoverLimits `json:"limits,omitempty"`
}

type ServiceError struct {
	Code          string         `json:"code"`
	Message       string         `json:"message"`
	Category      string         `json:"category,omitempty"`
	RetryStrategy string         `json:"retry_strategy,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
}

type Crawl4AiResponse struct {
	URL          string          `json:"url"`
	FinalURL     string          `json:"final_url"`
	Status       string          `json:"status"`
	Markdown     string          `json:"markdown,omitempty"`
	MarkdownHash string          `json:"markdown_hash,omitempty"`
	Links        []string        `json:"links,omitempty"`
	LLMOutput    map[string]any  `json:"llm_output,omitempty"`
	Error        map[string]any  `json:"error,omitempty"`
	DurationMS   int             `json:"duration_ms"`
	Metadata     map[string]any  `json:"metadata,omitempty"`
	RawPayload   json.RawMessage `json:"-"`
}

type SearchFetchRequest struct {
	SearchTerm     string `json:"search_term"`
	SearchEngine   string `json:"search_engine,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

type SearchAnalyzeRequest struct {
	CompanyName        string   `json:"company_name"`
	OrganizationNumber string   `json:"organization_number,omitempty"`
	Country            string   `json:"country,omitempty"`
	AddressLines       []string `json:"address_lines,omitempty"`
	City               string   `json:"city,omitempty"`
	PostalCode         string   `json:"postal_code,omitempty"`
	BusinessActivity   []string `json:"business_activity,omitempty"`
	StatutoryPurpose   []string `json:"statutory_purpose,omitempty"`
	IndustryCodes      []string `json:"industry_codes,omitempty"`
	SearchEngine       string   `json:"search_engine,omitempty"`
	SearchTerm         string   `json:"search_term"`
	Links              []string `json:"links,omitempty"`
	Markdown           string   `json:"markdown,omitempty"`
	CandidateThreshold int      `json:"candidate_threshold,omitempty"`
	MaxCandidates      int      `json:"max_candidates,omitempty"`
	TimeoutSeconds     int      `json:"timeout_seconds,omitempty"`
}

type CrawlPageRequest struct {
	URL            string         `json:"url"`
	TimeoutSeconds int            `json:"timeout_seconds,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type PageAnalyzeRequest struct {
	CompanyName        string   `json:"company_name"`
	OrganizationNumber string   `json:"organization_number,omitempty"`
	Country            string   `json:"country,omitempty"`
	AddressLines       []string `json:"address_lines,omitempty"`
	City               string   `json:"city,omitempty"`
	PostalCode         string   `json:"postal_code,omitempty"`
	BusinessActivity   []string `json:"business_activity,omitempty"`
	StatutoryPurpose   []string `json:"statutory_purpose,omitempty"`
	IndustryCodes      []string `json:"industry_codes,omitempty"`
	URL                string   `json:"url"`
	FinalURL           string   `json:"final_url"`
	NormalizedDomain   string   `json:"normalized_domain"`
	Markdown           string   `json:"markdown,omitempty"`
	CandidateScore     int      `json:"candidate_score,omitempty"`
	CandidateReason    string   `json:"candidate_reason,omitempty"`
	TimeoutSeconds     int      `json:"timeout_seconds,omitempty"`
}

type ScoredLink struct {
	URL              string         `json:"url"`
	Domain           string         `json:"domain"`
	NormalizedDomain string         `json:"normalized_domain"`
	Score            int            `json:"score"`
	Reason           string         `json:"reason,omitempty"`
	Evidence         []string       `json:"evidence,omitempty"`
	Source           string         `json:"source,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

type SearchAnalyzeResponse struct {
	Status     string          `json:"status"`
	Candidates []ScoredLink    `json:"candidates,omitempty"`
	Error      *ServiceError   `json:"error,omitempty"`
	RawPayload json.RawMessage `json:"-"`
}

type PageAnalyzeResponse struct {
	Status     string          `json:"status"`
	Analysis   map[string]any  `json:"analysis,omitempty"`
	Error      *ServiceError   `json:"error,omitempty"`
	RawPayload json.RawMessage `json:"-"`
}

type BrregDomainDiscoveryStatus string

const (
	BrregDomainDiscoveryStatusSucceeded BrregDomainDiscoveryStatus = "succeeded"
	BrregDomainDiscoveryStatusNotFound  BrregDomainDiscoveryStatus = "not_found"
	BrregDomainDiscoveryStatusSkipped   BrregDomainDiscoveryStatus = "skipped"
	BrregDomainDiscoveryStatusFailed    BrregDomainDiscoveryStatus = "failed"
)

type BrregDomainDiscoveryResponse struct {
	SchemaVersion  string          `json:"schema_version"`
	Status         string          `json:"status"`
	BestDomain     *string         `json:"best_domain,omitempty"`
	SearchEngine   *string         `json:"search_engine,omitempty"`
	SearchTerm     *string         `json:"search_term,omitempty"`
	Errors         []ServiceError  `json:"errors,omitempty"`
	Warnings       []ServiceError  `json:"warnings,omitempty"`
	DurationMS     int             `json:"duration_ms"`
	ServiceVersion string          `json:"service_version,omitempty"`
	RawPayload     json.RawMessage `json:"-"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
		timeout: 5 * time.Minute,
	}
}

func (c *Client) DiscoverBrregDomain(ctx context.Context, request BrregDomainDiscoveryRequest) (BrregDomainDiscoveryResponse, error) {
	var decoded BrregDomainDiscoveryResponse
	payload, err := c.postJSON(ctx, "/v1/brreg/domain-discovery", "discover brreg domain", request, &decoded)
	if err != nil {
		return BrregDomainDiscoveryResponse{}, err
	}
	decoded.RawPayload = json.RawMessage(payload)
	return decoded, nil
}

func (c *Client) FetchSearchPage(ctx context.Context, request SearchFetchRequest) (Crawl4AiResponse, error) {
	var decoded Crawl4AiResponse
	payload, err := c.postJSON(ctx, "/v1/search/fetch", "fetch crawl service search page", request, &decoded)
	if err != nil {
		return Crawl4AiResponse{}, err
	}
	decoded.RawPayload = json.RawMessage(payload)
	return decoded, nil
}

func (c *Client) AnalyzeSearchPage(ctx context.Context, request SearchAnalyzeRequest) (SearchAnalyzeResponse, error) {
	var decoded SearchAnalyzeResponse
	payload, err := c.postJSON(ctx, "/v1/search/analyze", "analyze crawl service search page", request, &decoded)
	if err != nil {
		return SearchAnalyzeResponse{}, err
	}
	decoded.RawPayload = json.RawMessage(payload)
	return decoded, nil
}

func (c *Client) CrawlPage(ctx context.Context, request CrawlPageRequest) (Crawl4AiResponse, error) {
	var decoded Crawl4AiResponse
	payload, err := c.postJSON(ctx, "/v1/pages/crawl", "crawl service page crawl", request, &decoded)
	if err != nil {
		return Crawl4AiResponse{}, err
	}
	decoded.RawPayload = json.RawMessage(payload)
	return decoded, nil
}

func (c *Client) AnalyzeCompanyPage(ctx context.Context, request PageAnalyzeRequest) (PageAnalyzeResponse, error) {
	var decoded PageAnalyzeResponse
	payload, err := c.postJSON(ctx, "/v1/pages/analyze-company", "analyze crawl service company page", request, &decoded)
	if err != nil {
		return PageAnalyzeResponse{}, err
	}
	decoded.RawPayload = json.RawMessage(payload)
	return decoded, nil
}

func (c *Client) postJSON(ctx context.Context, path string, operation string, request any, response any) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal %s request", operation)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrapf(err, "create %s request", operation)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, operation)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
		return nil, errors.Wrapf(ErrUnexpectedStatus, "status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(payload)))
	}

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return nil, errors.Wrapf(err, "read %s response", operation)
	}
	if response != nil {
		if err := json.Unmarshal(payload, response); err != nil {
			return nil, errors.Wrapf(err, "decode %s response", operation)
		}
	}
	return payload, nil
}
