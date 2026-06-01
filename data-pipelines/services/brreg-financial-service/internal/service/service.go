package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/pulsarpoint/brreg-financial-service/internal/parser"
)

var orgNumRegexp = regexp.MustCompile(`^\d{9}$`)

// BRREGClient is the interface the service requires from the HTTP client.
type BRREGClient interface {
	FetchKeyFigures(ctx context.Context, orgNum string) ([]byte, error)
	FetchPDFYears(ctx context.Context, orgNum string) ([]string, error)
}

// Config holds Service construction parameters.
type Config struct {
	BaseURL      string
	MaxBatchSize int
}

// Service orchestrates brregclient and parser to produce LookupResponse values.
type Service struct {
	client BRREGClient
	cfg    Config
}

// New creates a Service.
func New(client BRREGClient, cfg Config) *Service {
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = 1000
	}
	return &Service{client: client, cfg: cfg}
}

// Lookup processes a batch of organization lookups.
// Returns an error only for request-level failures (batch too large, invalid org number).
// Per-record failures are encoded in RecordResult.Status.
func (s *Service) Lookup(ctx context.Context, req models.LookupRequest) (models.LookupResponse, error) {
	if len(req.Records) > s.cfg.MaxBatchSize {
		return models.LookupResponse{}, fmt.Errorf("batch_too_large: max %d, got %d", s.cfg.MaxBatchSize, len(req.Records))
	}

	// Validate org numbers before making any HTTP calls.
	for _, rec := range req.Records {
		norm := normalizeOrgNum(rec.OrganizationNumber)
		if !orgNumRegexp.MatchString(norm) {
			return models.LookupResponse{}, fmt.Errorf("invalid_organization_number: %q", rec.OrganizationNumber)
		}
	}

	start := time.Now()
	results := make([]models.RecordResult, 0, len(req.Records))
	completed, failed := 0, 0

	for _, rec := range req.Records {
		result := s.lookupOne(ctx, rec, req.IncludePDFMetadata, req.IncludeRawPayload)
		results = append(results, result)
		if result.Status == "failed" {
			failed++
		} else {
			completed++
		}
	}

	return models.LookupResponse{
		SchemaVersion:    models.SchemaVersion,
		Status:           batchStatus(completed, failed),
		RecordsSeen:      len(req.Records),
		RecordsCompleted: completed,
		RecordsFailed:    failed,
		DurationMs:       time.Since(start).Milliseconds(),
		Results:          results,
	}, nil
}

func (s *Service) lookupOne(ctx context.Context, rec models.LookupRecord, includePDF, includeRaw bool) models.RecordResult {
	orgNum := normalizeOrgNum(rec.OrganizationNumber)
	result := models.RecordResult{
		RecordID:           rec.RecordID,
		OrganizationNumber: orgNum,
		Statements:         []models.Statement{},
		Warnings:           []models.Warning{},
	}

	rawBody, err := s.client.FetchKeyFigures(ctx, orgNum)
	switch {
	case err == nil:
		stmts, parseErr := parser.ParseKeyFigures(rawBody, orgNum, s.cfg.BaseURL)
		if parseErr != nil {
			result.Status = "failed"
			result.Warnings = append(result.Warnings, models.Warning{
				Code:    "parse_error",
				Message: parseErr.Error(),
			})
		} else {
			if !includeRaw {
				for i := range stmts {
					stmts[i].RawPayload = nil
				}
			}
			result.Status = "succeeded"
			result.Statements = stmts
		}

	case errors.Is(err, brregclient.ErrNotAvailable):
		result.Status = "not_available"
		result.Warnings = append(result.Warnings, models.Warning{
			Code:    "financials_not_found",
			Message: "No BRREG annual-account key figures are available",
		})

	default:
		var planErr *brregclient.UnsupportedPlanError
		if errors.As(err, &planErr) {
			result.Status = "unsupported_statement_plan"
			result.Warnings = append(result.Warnings, models.Warning{
				Code:    "unsupported_statement_plan",
				Message: "BRREG key-figure API does not support this statement plan",
				Detail:  map[string]any{"statement_plan": planErr.PlanName},
			})
		} else {
			result.Status = "failed"
			result.Warnings = append(result.Warnings, models.Warning{
				Code:    "provider_unavailable",
				Message: err.Error(),
				Detail:  map[string]any{"category": "external_service", "retry_strategy": "retry_with_backoff"},
			})
		}
	}

	if includePDF {
		result.PDFMetadata = s.fetchPDFMetadata(ctx, orgNum)
	}

	return result
}

func (s *Service) fetchPDFMetadata(ctx context.Context, orgNum string) *models.PDFMetadata {
	years, err := s.client.FetchPDFYears(ctx, orgNum)
	if err != nil {
		return &models.PDFMetadata{AvailableYears: []string{}}
	}
	if years == nil {
		years = []string{}
	}
	return &models.PDFMetadata{
		AvailableYears:      years,
		DownloadURLTemplate: fmt.Sprintf("%s/regnskapsregisteret/regnskap/aarsregnskap/kopi/%s/{year}", s.cfg.BaseURL, orgNum),
	}
}

func batchStatus(completed, failed int) string {
	switch {
	case failed == 0:
		return "succeeded"
	case completed == 0:
		return "failed"
	default:
		return "partial"
	}
}

func normalizeOrgNum(s string) string {
	out := make([]byte, 0, 9)
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			out = append(out, s[i])
		}
	}
	return string(out)
}
