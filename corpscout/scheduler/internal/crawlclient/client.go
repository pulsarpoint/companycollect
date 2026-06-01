package crawlclient

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

const (
	DefaultSearchFetchSubject   = "brreg.domain.search.fetch"
	DefaultSearchAnalyzeSubject = "brreg.domain.search.analyze"
)

type LLMSelection struct {
	Provider string `json:"provider"`
	Model    string `json:"model,omitempty"`
	BaseURL  string `json:"base_url,omitempty"`
	APIKey   string `json:"api_key,omitempty"`
}

type SearchFetchRequest struct {
	SearchTerm     string `json:"search_term"`
	SearchEngine   string `json:"search_engine"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type Crawl4AiResponse struct {
	URL          string         `json:"url"`
	FinalURL     string         `json:"final_url"`
	Status       string         `json:"status"`
	Markdown     string         `json:"markdown,omitempty"`
	MarkdownHash string         `json:"markdown_hash,omitempty"`
	Links        []string       `json:"links"`
	LLMOutput    map[string]any `json:"llm_output,omitempty"`
	Error        map[string]any `json:"error,omitempty"`
	DurationMS   int            `json:"duration_ms"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

type SearchAnalyzeRequest struct {
	CompanyName        string       `json:"company_name"`
	OrganizationNumber string       `json:"organization_number,omitempty"`
	Country            string       `json:"country"`
	AddressLines       []string     `json:"address_lines"`
	City               string       `json:"city,omitempty"`
	PostalCode         string       `json:"postal_code,omitempty"`
	BusinessActivity   []string     `json:"business_activity"`
	StatutoryPurpose   []string     `json:"statutory_purpose"`
	IndustryCodes      []string     `json:"industry_codes"`
	SearchEngine       string       `json:"search_engine"`
	SearchTerm         string       `json:"search_term"`
	Links              []string     `json:"links"`
	Markdown           string       `json:"markdown"`
	CandidateThreshold int          `json:"candidate_threshold"`
	MaxCandidates      int          `json:"max_candidates"`
	TimeoutSeconds     int          `json:"timeout_seconds"`
	LLM                LLMSelection `json:"llm"`
}

type ScoredLink struct {
	URL              string         `json:"url"`
	Domain           string         `json:"domain"`
	NormalizedDomain string         `json:"normalized_domain"`
	Score            int            `json:"score"`
	Reason           string         `json:"reason,omitempty"`
	Evidence         []string       `json:"evidence"`
	Source           string         `json:"source,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

type ServiceError struct {
	Code          string         `json:"code"`
	Message       string         `json:"message"`
	Category      string         `json:"category,omitempty"`
	RetryStrategy string         `json:"retry_strategy,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
}

type SearchAnalyzeResponse struct {
	Status     string        `json:"status"`
	Candidates []ScoredLink  `json:"candidates"`
	Error      *ServiceError `json:"error,omitempty"`
}

type natsRequester interface {
	Request(context.Context, string, []byte) ([]byte, error)
}

type Client struct {
	searchFetchSubject   string
	searchAnalyzeSubject string
	requester            natsRequester
	conn                 *nats.Conn
}

func NewNATS(url string) (*Client, error) {
	return NewNATSWithSubjects(url, DefaultSearchFetchSubject, DefaultSearchAnalyzeSubject)
}

func NewNATSWithSubjects(url string, searchFetchSubject string, searchAnalyzeSubject string) (*Client, error) {
	if searchFetchSubject == "" {
		searchFetchSubject = DefaultSearchFetchSubject
	}
	if searchAnalyzeSubject == "" {
		searchAnalyzeSubject = DefaultSearchAnalyzeSubject
	}
	slog.Debug("connecting brreg crawl nats client",
		"search_fetch_subject", searchFetchSubject,
		"search_analyze_subject", searchAnalyzeSubject,
	)
	conn, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "connect to nats")
	}
	slog.Debug("connected brreg crawl nats client",
		"search_fetch_subject", searchFetchSubject,
		"search_analyze_subject", searchAnalyzeSubject,
	)
	return &Client{
		searchFetchSubject:   searchFetchSubject,
		searchAnalyzeSubject: searchAnalyzeSubject,
		requester:            natsConnRequester{conn: conn},
		conn:                 conn,
	}, nil
}

func newClientFromRequester(searchFetchSubject string, searchAnalyzeSubject string, requester natsRequester) *Client {
	if searchFetchSubject == "" {
		searchFetchSubject = DefaultSearchFetchSubject
	}
	if searchAnalyzeSubject == "" {
		searchAnalyzeSubject = DefaultSearchAnalyzeSubject
	}
	return &Client{
		searchFetchSubject:   searchFetchSubject,
		searchAnalyzeSubject: searchAnalyzeSubject,
		requester:            requester,
	}
}

func (c *Client) FetchSearchPage(ctx context.Context, request SearchFetchRequest) (Crawl4AiResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return Crawl4AiResponse{}, errors.Wrap(err, "encode brreg search fetch nats request")
	}
	slog.DebugContext(ctx, "requesting brreg search fetch over nats",
		"subject", c.searchFetchSubject,
		"search_engine", request.SearchEngine,
		"timeout_seconds", request.TimeoutSeconds,
		"payload_bytes", len(payload),
	)
	responsePayload, err := c.requester.Request(ctx, c.searchFetchSubject, payload)
	if err != nil {
		return Crawl4AiResponse{}, errors.Wrap(err, "request brreg search fetch over nats")
	}
	var decoded Crawl4AiResponse
	if err := json.Unmarshal(responsePayload, &decoded); err != nil {
		return Crawl4AiResponse{}, errors.Wrap(err, "decode brreg search fetch nats response")
	}
	slog.DebugContext(ctx, "received brreg search fetch nats response",
		"subject", c.searchFetchSubject,
		"status", decoded.Status,
		"links_count", len(decoded.Links),
		"markdown_hash", decoded.MarkdownHash,
		"response_bytes", len(responsePayload),
	)
	return decoded, nil
}

func (c *Client) AnalyzeSearchPage(ctx context.Context, request SearchAnalyzeRequest) (SearchAnalyzeResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return SearchAnalyzeResponse{}, errors.Wrap(err, "encode brreg search analysis nats request")
	}
	slog.DebugContext(ctx, "requesting brreg search analysis over nats",
		"subject", c.searchAnalyzeSubject,
		"search_engine", request.SearchEngine,
		"candidate_threshold", request.CandidateThreshold,
		"max_candidates", request.MaxCandidates,
		"provider", request.LLM.Provider,
		"model", request.LLM.Model,
		"has_inline_base_url", request.LLM.BaseURL != "",
		"has_inline_api_key", request.LLM.APIKey != "",
		"payload_bytes", len(payload),
	)
	responsePayload, err := c.requester.Request(ctx, c.searchAnalyzeSubject, payload)
	if err != nil {
		return SearchAnalyzeResponse{}, errors.Wrap(err, "request brreg search analysis over nats")
	}
	var decoded SearchAnalyzeResponse
	if err := json.Unmarshal(responsePayload, &decoded); err != nil {
		return SearchAnalyzeResponse{}, errors.Wrap(err, "decode brreg search analysis nats response")
	}
	slog.DebugContext(ctx, "received brreg search analysis nats response",
		"subject", c.searchAnalyzeSubject,
		"status", decoded.Status,
		"candidates_count", len(decoded.Candidates),
		"response_bytes", len(responsePayload),
	)
	return decoded, nil
}

func (c *Client) Close() {
	if c != nil && c.conn != nil {
		slog.Debug("closing brreg crawl nats connection",
			"search_fetch_subject", c.searchFetchSubject,
			"search_analyze_subject", c.searchAnalyzeSubject,
		)
		c.conn.Close()
	}
}

type natsConnRequester struct {
	conn *nats.Conn
}

func (r natsConnRequester) Request(ctx context.Context, subject string, payload []byte) ([]byte, error) {
	msg, err := r.conn.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}
