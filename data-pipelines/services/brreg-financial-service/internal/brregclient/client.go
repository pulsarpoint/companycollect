package brregclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/parser"
)

// ErrNotAvailable is returned when BRREG responds 404 for an org number.
var ErrNotAvailable = errors.New("brreg: key figures not available")

// UnsupportedPlanError is returned when BRREG responds 500 with an unsupported statement plan.
type UnsupportedPlanError struct {
	PlanName string
}

func (e *UnsupportedPlanError) Error() string {
	return fmt.Sprintf("brreg: unsupported statement plan %q", e.PlanName)
}

// RetryableError is returned on transient failures (HTTP 429, 5xx, network).
type RetryableError struct {
	StatusCode int
	Msg        string
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("brreg: retryable error (HTTP %d): %s", e.StatusCode, e.Msg)
}

// Config holds client construction parameters.
type Config struct {
	BaseURL        string
	RequestTimeout time.Duration
}

// Client is an HTTP client for the BRREG Regnskapsregister API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a Client. Config.BaseURL defaults to "https://data.brreg.no".
func New(cfg Config) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://data.brreg.no"
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		baseURL: cfg.BaseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// FetchKeyFigures fetches the key-figure list for an org number.
// Returns raw response bytes on HTTP 200.
// Returns ErrNotAvailable on HTTP 404.
// Returns *UnsupportedPlanError on HTTP 500 with known plan error.
// Returns *RetryableError on HTTP 429 or other 5xx.
func (c *Client) FetchKeyFigures(ctx context.Context, orgNum string) ([]byte, error) {
	url := fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s", c.baseURL, orgNum)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build key figures request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &RetryableError{StatusCode: 0, Msg: err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: "read body: " + err.Error()}
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		return body, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotAvailable
	case resp.StatusCode == http.StatusInternalServerError:
		if plan, ok := parser.ParseUnsupportedPlan(body); ok {
			return nil, &UnsupportedPlanError{PlanName: plan}
		}
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: string(body)}
	default:
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: fmt.Sprintf("unexpected status %d", resp.StatusCode)}
	}
}

// FetchPDFYears fetches available annual-account PDF years for an org number.
// Returns ErrNotAvailable on HTTP 404.
func (c *Client) FetchPDFYears(ctx context.Context, orgNum string) ([]string, error) {
	url := fmt.Sprintf("%s/regnskapsregisteret/regnskap/aarsregnskap/kopi/%s/aar", c.baseURL, orgNum)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build pdf years request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &RetryableError{StatusCode: 0, Msg: err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: "read body: " + err.Error()}
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotAvailable
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &RetryableError{StatusCode: resp.StatusCode, Msg: fmt.Sprintf("unexpected status %d", resp.StatusCode)}
	}

	return parser.ParsePDFYears(body)
}
