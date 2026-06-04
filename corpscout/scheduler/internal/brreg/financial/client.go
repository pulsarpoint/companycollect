package financial

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var ErrNotAvailable = errNotAvailable{}

type errNotAvailable struct{}

func (errNotAvailable) Error() string {
	return "brreg financial data not available"
}

type UnsupportedPlanError struct {
	PlanName string
}

func (e *UnsupportedPlanError) Error() string {
	return fmt.Sprintf("brreg financial statement plan %q is not supported", e.PlanName)
}

type RetryableError struct {
	StatusCode int
	Message    string
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("brreg financial retryable error status=%d message=%s", e.StatusCode, e.Message)
}

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{baseURL: baseURL, http: httpClient}
}

func (c *Client) LookupRecords(ctx context.Context, records []LookupRecord) ([]RecordResult, error) {
	results := make([]RecordResult, 0, len(records))
	for _, record := range records {
		result, err := c.LookupRecord(ctx, record)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (c *Client) LookupRecord(ctx context.Context, record LookupRecord) (RecordResult, error) {
	raw, err := c.fetchKeyFigures(ctx, record.OrganizationNumber)
	if err != nil {
		switch typed := err.(type) {
		case errNotAvailable:
			return RecordResult{
				RecordID:           record.RecordID,
				OrganizationNumber: record.OrganizationNumber,
				Status:             StatusNotAvailable,
				Warnings: []Warning{{
					Code:    "financials_not_found",
					Message: "BRREG has no annual-account key figures for this organization.",
				}},
			}, nil
		case *UnsupportedPlanError:
			return RecordResult{
				RecordID:           record.RecordID,
				OrganizationNumber: record.OrganizationNumber,
				Status:             StatusUnsupportedStatementPlan,
				Warnings: []Warning{{
					Code:    "unsupported_statement_plan",
					Message: typed.Error(),
					Detail:  map[string]any{"plan_name": typed.PlanName},
				}},
			}, nil
		default:
			return RecordResult{}, err
		}
	}
	statements, err := ParseKeyFigures(raw, record.OrganizationNumber, c.baseURL)
	if err != nil {
		return RecordResult{}, fmt.Errorf("parse brreg financial key figures: %w", err)
	}
	return RecordResult{
		RecordID:           record.RecordID,
		OrganizationNumber: record.OrganizationNumber,
		Status:             StatusSucceeded,
		Statements:         statements,
	}, nil
}

func (c *Client) fetchKeyFigures(ctx context.Context, orgNumber string) ([]byte, error) {
	url := fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s", c.baseURL, orgNumber)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build brreg financial request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return nil, &RetryableError{StatusCode: 0, Message: err.Error()}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &RetryableError{StatusCode: response.StatusCode, Message: "read response body: " + err.Error()}
	}
	switch {
	case response.StatusCode == http.StatusOK:
		return body, nil
	case response.StatusCode == http.StatusNotFound:
		return nil, ErrNotAvailable
	case response.StatusCode == http.StatusInternalServerError:
		if planName, ok := ParseUnsupportedPlan(body); ok {
			return nil, &UnsupportedPlanError{PlanName: planName}
		}
		return nil, &RetryableError{StatusCode: response.StatusCode, Message: string(body)}
	case response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError:
		return nil, &RetryableError{StatusCode: response.StatusCode, Message: string(body)}
	default:
		return nil, &RetryableError{StatusCode: response.StatusCode, Message: fmt.Sprintf("unexpected status %d", response.StatusCode)}
	}
}
