package translationclient

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

const DefaultBrregTranslationSubject = "brreg.translation.translate"

type LLMSelection struct {
	Provider string `json:"provider"`
	Model    string `json:"model,omitempty"`
	BaseURL  string `json:"base_url,omitempty"`
	APIKey   string `json:"api_key,omitempty"`
}

type BrregRecord struct {
	RecordID           string          `json:"record_id"`
	OrganizationNumber string          `json:"organization_number"`
	RawPayload         json.RawMessage `json:"raw_payload"`
}

type BrregTranslateRequest struct {
	Records       []BrregRecord `json:"records"`
	LLM           LLMSelection  `json:"llm"`
	PromptVersion string        `json:"prompt_version"`
	SourceLang    string        `json:"source_lang"`
	TargetLang    string        `json:"target_lang"`
	MaxRetries    int           `json:"max_retries"`
}

type TranslationError struct {
	Code          string         `json:"code"`
	Message       string         `json:"message"`
	Category      string         `json:"category,omitempty"`
	RetryStrategy string         `json:"retry_strategy,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
}

type BrregTranslateRecordStatus string

const (
	BrregTranslateStatusSucceeded BrregTranslateRecordStatus = "succeeded"
	BrregTranslateStatusSkipped   BrregTranslateRecordStatus = "skipped"
	BrregTranslateStatusFailed    BrregTranslateRecordStatus = "failed"
)

type BrregRecordTranslationResult struct {
	RecordID           string            `json:"record_id"`
	OrganizationNumber string            `json:"organization_number"`
	Status             string            `json:"status"`
	TranslatedPayload  map[string]any    `json:"translated_payload,omitempty"`
	MissingTerms       []string          `json:"missing_terms"`
	Error              *TranslationError `json:"error,omitempty"`
	DurationMS         int               `json:"duration_ms"`
}

type BrregTranslateResponse struct {
	SchemaVersion    string                         `json:"schema_version"`
	Status           string                         `json:"status"`
	Provider         string                         `json:"provider"`
	Model            string                         `json:"model"`
	PromptVersion    string                         `json:"prompt_version"`
	RecordsSeen      int                            `json:"records_seen"`
	RecordsCompleted int                            `json:"records_completed"`
	RecordsFailed    int                            `json:"records_failed"`
	RecordsSkipped   int                            `json:"records_skipped"`
	DurationMS       int                            `json:"duration_ms"`
	Results          []BrregRecordTranslationResult `json:"results"`
}

type natsRequester interface {
	Request(context.Context, string, []byte) ([]byte, error)
}

type Client struct {
	subject        string
	requestTimeout time.Duration
	requester      natsRequester
	conn           *nats.Conn
}

func NewNATS(url string) (*Client, error) {
	return NewNATSWithSubject(url, DefaultBrregTranslationSubject)
}

func NewNATSWithSubject(url string, subject string) (*Client, error) {
	return NewNATSWithSubjectAndTimeout(url, subject, 0)
}

func NewNATSWithRequestTimeout(url string, requestTimeout time.Duration) (*Client, error) {
	return NewNATSWithSubjectAndTimeout(url, DefaultBrregTranslationSubject, requestTimeout)
}

func NewNATSWithSubjectAndTimeout(url string, subject string, requestTimeout time.Duration) (*Client, error) {
	if subject == "" {
		subject = DefaultBrregTranslationSubject
	}
	slog.Debug("connecting brreg translation nats client",
		"subject", subject,
		"request_timeout", requestTimeout.String(),
	)
	conn, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "connect to nats")
	}
	slog.Debug("connected brreg translation nats client",
		"subject", subject,
		"request_timeout", requestTimeout.String(),
	)
	return &Client{
		subject:        subject,
		requestTimeout: requestTimeout,
		requester:      natsConnRequester{conn: conn},
		conn:           conn,
	}, nil
}

func newClientFromRequester(subject string, requester natsRequester) *Client {
	if subject == "" {
		subject = DefaultBrregTranslationSubject
	}
	return &Client{subject: subject, requester: requester}
}

func (c *Client) TranslateBrregRecords(ctx context.Context, request BrregTranslateRequest) (BrregTranslateResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return BrregTranslateResponse{}, errors.Wrap(err, "encode brreg translation nats request")
	}
	slog.DebugContext(ctx, "requesting brreg translation over nats",
		"subject", c.subject,
		"records_count", len(request.Records),
		"provider", request.LLM.Provider,
		"model", request.LLM.Model,
		"has_inline_base_url", request.LLM.BaseURL != "",
		"has_inline_api_key", request.LLM.APIKey != "",
		"prompt_version", request.PromptVersion,
		"payload_bytes", len(payload),
		"request_timeout", c.requestTimeout.String(),
	)
	requestCtx, cancel := natsRequestContext(ctx, c.requestTimeout)
	defer cancel()
	responsePayload, err := c.requester.Request(requestCtx, c.subject, payload)
	if err != nil {
		return BrregTranslateResponse{}, errors.Wrap(err, "request brreg translation over nats")
	}
	var decoded BrregTranslateResponse
	if err := json.Unmarshal(responsePayload, &decoded); err != nil {
		return BrregTranslateResponse{}, errors.Wrap(err, "decode brreg translation nats response")
	}
	slog.DebugContext(ctx, "received brreg translation nats response",
		"subject", c.subject,
		"status", decoded.Status,
		"records_seen", decoded.RecordsSeen,
		"records_completed", decoded.RecordsCompleted,
		"records_failed", decoded.RecordsFailed,
		"records_skipped", decoded.RecordsSkipped,
		"response_bytes", len(responsePayload),
	)
	return decoded, nil
}

func (c *Client) TranslateTerms(ctx context.Context, request TermTranslationRequest) (TermTranslationResult, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return TermTranslationResult{}, errors.Wrap(err, "encode term translation nats request")
	}
	subject := c.subject
	if subject == "" || subject == DefaultBrregTranslationSubject {
		subject = DefaultTermTranslationRequestSubject
	}
	slog.DebugContext(ctx, "requesting term translation over nats",
		"subject", subject,
		"request_id", request.RequestID,
		"terms_count", len(request.Terms),
		"provider", request.Provider,
		"model", request.Model,
		"prompt_version", request.PromptVersion,
		"payload_bytes", len(payload),
		"request_timeout", c.requestTimeout.String(),
	)
	requestCtx, cancel := natsRequestContext(ctx, c.requestTimeout)
	defer cancel()
	responsePayload, err := c.requester.Request(requestCtx, subject, payload)
	if err != nil {
		return TermTranslationResult{}, errors.Wrap(err, "request term translation over nats")
	}
	var decoded TermTranslationResult
	if err := json.Unmarshal(responsePayload, &decoded); err != nil {
		return TermTranslationResult{}, errors.Wrap(err, "decode term translation nats response")
	}
	slog.DebugContext(ctx, "received term translation nats response",
		"subject", subject,
		"request_id", decoded.RequestID,
		"results", len(decoded.Results),
		"failures", len(decoded.Failures),
		"provider", decoded.Provider,
		"model", decoded.Model,
		"prompt_version", decoded.PromptVersion,
		"response_bytes", len(responsePayload),
	)
	return decoded, nil
}

func (c *Client) TranslateBrregTerms(ctx context.Context, request TermTranslationRequest) (TermTranslationResult, error) {
	return c.TranslateTerms(ctx, request)
}

func natsRequestContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (c *Client) Close() {
	if c != nil && c.conn != nil {
		slog.Debug("closing brreg translation nats connection", "subject", c.subject)
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
