package actions

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type CompanyTranslationActions struct {
	store      *companydata.Store
	translator *translationclient.Client
}

func NewCompanyTranslationActions(store *companydata.Store, translator *translationclient.Client) *CompanyTranslationActions {
	return &CompanyTranslationActions{store: store, translator: translator}
}

type ClaimBrregCompaniesForTranslationInput struct {
	Limit            int32  `json:"limit"`
	MaxParallelTasks int32  `json:"max_parallel_tasks"`
	LeaseSeconds     int32  `json:"lease_seconds"`
	MaxAttempts      int32  `json:"max_attempts"`
	WorkerID         string `json:"worker_id,omitempty"`
}

type ClaimedCompanyForTranslation struct {
	CompanyID          string `json:"company_id"`
	OrganizationNumber string `json:"organization_number"`
	OrganizationName   string `json:"organization_name"`
	AttemptCount       int32  `json:"attempt_count"`
}

type ClaimBrregCompaniesForTranslationResult struct {
	StatusRowsInserted int32                          `json:"status_rows_inserted"`
	Companies          []ClaimedCompanyForTranslation `json:"companies"`
}

type ProcessBrregCompanyTranslationInput struct {
	CompanyID     string `json:"company_id"`
	Provider      string `json:"provider,omitempty"`
	Model         string `json:"model,omitempty"`
	PromptVersion string `json:"prompt_version,omitempty"`
	MaxAttempts   int32  `json:"max_attempts,omitempty"`
}

type ProcessBrregCompanyTranslationResult struct {
	CompanyID       string `json:"company_id"`
	Status          string `json:"status"`
	FieldsSeen      int32  `json:"fields_seen"`
	FieldsApplied   int32  `json:"fields_applied"`
	RemainingFields int32  `json:"remaining_fields"`
	TermsRequested  int32  `json:"terms_requested"`
	TermsSucceeded  int32  `json:"terms_succeeded"`
	TermsFailed     int32  `json:"terms_failed"`
}

func (a *CompanyTranslationActions) ClaimBrregCompaniesForTranslation(
	ctx context.Context,
	input ClaimBrregCompaniesForTranslationInput,
) (ClaimBrregCompaniesForTranslationResult, error) {
	if a == nil || a.store == nil {
		return ClaimBrregCompaniesForTranslationResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.ClaimForTranslation(ctx, companydata.ClaimForTranslationCommand{
		Limit:            input.Limit,
		MaxParallelTasks: input.MaxParallelTasks,
		LeaseSeconds:     input.LeaseSeconds,
		MaxAttempts:      input.MaxAttempts,
		WorkerID:         input.WorkerID,
	})
	if err != nil {
		return ClaimBrregCompaniesForTranslationResult{}, errors.Wrap(err, "claim brreg companies for translation")
	}
	companies := make([]ClaimedCompanyForTranslation, 0, len(result.Companies))
	for _, company := range result.Companies {
		companies = append(companies, ClaimedCompanyForTranslation{
			CompanyID:          company.Company.ID.String(),
			OrganizationNumber: company.Company.OrganizationNumber,
			OrganizationName:   company.Company.OrganizationName,
			AttemptCount:       company.AttemptCount,
		})
	}
	slog.DebugContext(ctx, "claimed brreg companies for translation",
		"status_rows_inserted", result.StatusRowsInserted,
		"companies", len(companies),
	)
	return ClaimBrregCompaniesForTranslationResult{
		StatusRowsInserted: result.StatusRowsInserted,
		Companies:          companies,
	}, nil
}

func (a *CompanyTranslationActions) ProcessBrregCompanyTranslation(
	ctx context.Context,
	input ProcessBrregCompanyTranslationInput,
) (ProcessBrregCompanyTranslationResult, error) {
	if a == nil || a.store == nil {
		return ProcessBrregCompanyTranslationResult{}, errors.New("brreg companydata store not available")
	}
	companyID, err := uuid.Parse(input.CompanyID)
	if err != nil {
		return ProcessBrregCompanyTranslationResult{}, errors.Wrap(err, "parse brreg company id")
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultTranslationPromptVersion
	}
	if input.Provider == "" {
		input.Provider = "default"
	}
	result := ProcessBrregCompanyTranslationResult{
		CompanyID: input.CompanyID,
		Status:    "running",
	}
	slog.DebugContext(ctx, "processing brreg company translation",
		"company_id", input.CompanyID,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
	)

	applied, err := a.store.ApplyCachedTranslations(ctx, companydata.ApplyCachedTranslationsCommand{
		CompanyID:     companyID,
		PromptVersion: input.PromptVersion,
	})
	if err != nil {
		return a.failCompanyTranslation(
			ctx,
			result,
			companyID,
			input.MaxAttempts,
			"apply cached brreg company translations failed",
			"translation_cache",
			"apply_cached_failed",
			map[string]any{"error": err.Error()},
		)
	}
	result.FieldsSeen = applied.FieldsSeen
	result.FieldsApplied = applied.FieldsApplied
	result.RemainingFields = applied.RemainingFields

	switch {
	case applied.FieldsSeen == 0 && applied.RemainingFields == 0:
		if err := a.store.MarkTranslationSkipped(ctx, companydata.MarkTranslationStatusCommand{
			CompanyID: companyID,
			Metadata:  jsonMetadata(map[string]any{"reason": "no_translation_fields"}),
		}); err != nil {
			return result, errors.Wrap(err, "mark brreg company translation skipped")
		}
		result.Status = "skipped"
		return result, nil
	case applied.RemainingFields == 0:
		if err := a.store.MarkTranslationSucceeded(ctx, companydata.MarkTranslationStatusCommand{
			CompanyID: companyID,
			Metadata: jsonMetadata(map[string]any{
				"fields_seen":    applied.FieldsSeen,
				"fields_applied": applied.FieldsApplied,
				"source":         "translation_cache",
			}),
		}); err != nil {
			return result, errors.Wrap(err, "mark brreg company translation succeeded")
		}
		result.Status = "succeeded"
		return result, nil
	}

	data, err := a.store.Load(ctx, companyID)
	if err != nil {
		return result, errors.Wrap(err, "load brreg company for translation")
	}
	terms := data.TranslationTerms()
	if len(terms) == 0 {
		if err := a.store.MarkTranslationSucceeded(ctx, companydata.MarkTranslationStatusCommand{
			CompanyID: companyID,
			Metadata:  jsonMetadata(map[string]any{"source": "companydata"}),
		}); err != nil {
			return result, errors.Wrap(err, "mark brreg company translation succeeded")
		}
		result.RemainingFields = 0
		result.Status = "succeeded"
		return result, nil
	}
	result.TermsRequested = int32(len(terms))
	if a.translator == nil {
		return a.failCompanyTranslation(
			ctx,
			result,
			companyID,
			input.MaxAttempts,
			"brreg term translation client not available",
			"translation_service",
			"client_not_available",
			nil,
		)
	}

	response, err := a.translator.TranslateBrregTerms(ctx, companyTranslationRequest(input, terms))
	if err != nil {
		return a.failCompanyTranslation(
			ctx,
			result,
			companyID,
			input.MaxAttempts,
			"request brreg term translation failed",
			"translation_service",
			"request_failed",
			map[string]any{"error": err.Error()},
		)
	}
	termResults := companyTranslationTermResults(input, response)
	result.TermsSucceeded, result.TermsFailed = countCompanyTranslationTerms(termResults)
	if _, err := a.store.SaveTranslationTerms(ctx, termResults); err != nil {
		return result, errors.Wrap(err, "save brreg company translation terms")
	}

	applied, err = a.store.ApplyCachedTranslations(ctx, companydata.ApplyCachedTranslationsCommand{
		CompanyID:     companyID,
		PromptVersion: input.PromptVersion,
	})
	if err != nil {
		return result, errors.Wrap(err, "apply translated brreg company terms")
	}
	result.FieldsApplied += applied.FieldsApplied
	result.RemainingFields = applied.RemainingFields
	if applied.RemainingFields == 0 {
		if err := a.store.MarkTranslationSucceeded(ctx, companydata.MarkTranslationStatusCommand{
			CompanyID: companyID,
			Metadata: jsonMetadata(map[string]any{
				"terms_requested": result.TermsRequested,
				"terms_succeeded": result.TermsSucceeded,
				"terms_failed":    result.TermsFailed,
				"provider":        response.Provider,
				"model":           response.Model,
				"prompt_version":  response.PromptVersion,
			}),
		}); err != nil {
			return result, errors.Wrap(err, "mark brreg company translation succeeded")
		}
		result.Status = "succeeded"
		return result, nil
	}
	return a.failCompanyTranslation(
		ctx,
		result,
		companyID,
		input.MaxAttempts,
		"brreg term translation did not complete all company fields",
		"translation_service",
		"missing_translated_terms",
		map[string]any{
			"terms_requested":  result.TermsRequested,
			"terms_succeeded":  result.TermsSucceeded,
			"terms_failed":     result.TermsFailed,
			"remaining_fields": result.RemainingFields,
		},
	)
}

func (a *CompanyTranslationActions) failCompanyTranslation(
	ctx context.Context,
	result ProcessBrregCompanyTranslationResult,
	companyID uuid.UUID,
	maxAttempts int32,
	message string,
	category string,
	code string,
	metadata any,
) (ProcessBrregCompanyTranslationResult, error) {
	if err := a.store.MarkTranslationFailed(ctx, companydata.MarkTranslationFailedCommand{
		CompanyID:     companyID,
		Error:         message,
		ErrorCategory: category,
		ErrorCode:     code,
		RetryStrategy: "retry_with_backoff",
		MaxAttempts:   maxAttempts,
		Metadata:      jsonMetadata(metadata),
	}); err != nil {
		return result, errors.Wrap(err, "mark brreg company translation failed")
	}
	result.Status = "failed"
	return result, nil
}

func companyTranslationRequest(
	input ProcessBrregCompanyTranslationInput,
	terms []companydata.TranslationTerm,
) translationclient.TermTranslationRequest {
	request := translationclient.TermTranslationRequest{
		RequestID:     uuid.NewString(),
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Terms:         make([]translationclient.TermTranslationRequestTerm, 0, len(terms)),
	}
	for _, term := range terms {
		request.Terms = append(request.Terms, translationclient.TermTranslationRequestTerm{
			TermKey:              term.Key,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.NormalizedText,
		})
	}
	return request
}

func companyTranslationTermResults(
	input ProcessBrregCompanyTranslationInput,
	response translationclient.TermTranslationResult,
) []companydata.TranslationTermResult {
	provider := defaultString(response.Provider, input.Provider)
	model := defaultString(response.Model, input.Model)
	promptVersion := defaultString(response.PromptVersion, input.PromptVersion)
	results := make([]companydata.TranslationTermResult, 0, len(response.Results)+len(response.Failures))
	for _, item := range response.Results {
		results = append(results, companydata.TranslationTermResult{
			SourceText:           item.SourceText,
			SourceTextNormalized: item.SourceTextNormalized,
			TermKey:              item.TermKey,
			TranslatedText:       item.TranslatedText,
			Status:               defaultString(item.Status, "succeeded"),
			Provider:             provider,
			Model:                model,
			PromptVersion:        promptVersion,
		})
	}
	for _, failure := range response.Failures {
		results = append(results, companydata.TranslationTermResult{
			SourceText:           failure.SourceText,
			SourceTextNormalized: failure.SourceTextNormalized,
			TermKey:              failure.TermKey,
			Status:               defaultString(failure.Status, "failed_retryable"),
			Provider:             provider,
			Model:                model,
			PromptVersion:        promptVersion,
			Error:                failure.Error,
			ErrorCode:            failure.ErrorCode,
		})
	}
	return results
}

func countCompanyTranslationTerms(terms []companydata.TranslationTermResult) (succeeded int32, failed int32) {
	for _, term := range terms {
		switch term.Status {
		case "succeeded":
			succeeded++
		default:
			failed++
		}
	}
	return succeeded, failed
}

func jsonMetadata(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage(`{}`)
	}
	metadata, err := json.Marshal(value)
	if err != nil || len(metadata) == 0 || string(metadata) == "null" {
		return json.RawMessage(`{}`)
	}
	return metadata
}
