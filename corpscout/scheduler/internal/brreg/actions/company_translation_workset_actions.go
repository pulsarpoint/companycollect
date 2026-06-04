package actions

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type BuildBrregTranslationWorksetInput struct {
	Path          string            `json:"path"`
	PromptVersion string            `json:"prompt_version,omitempty"`
	IDs           []string          `json:"ids,omitempty"`
	Filters       map[string]string `json:"filters,omitempty"`
	CompanyLimit  int32             `json:"company_limit,omitempty"`
	FieldLimit    int32             `json:"field_limit,omitempty"`
}

type BuildBrregTranslationWorksetResult = companydata.BuildTranslationWorksetResult

type ClaimBrregTranslationWorksetBatchInput struct {
	Path            string `json:"path"`
	MaxRequestChars int32  `json:"max_request_chars,omitempty"`
	MaxTerms        int32  `json:"max_terms,omitempty"`
	MaxAttempts     int32  `json:"max_attempts,omitempty"`
}

type TranslationWorksetTerm = companydata.TranslationWorksetTerm
type ClaimBrregTranslationWorksetBatchResult = companydata.ClaimTranslationWorksetBatchResult

type TranslateBrregTranslationWorksetBatchInput struct {
	BatchID       int64                    `json:"batch_id"`
	Terms         []TranslationWorksetTerm `json:"terms"`
	Provider      string                   `json:"provider,omitempty"`
	Model         string                   `json:"model,omitempty"`
	PromptVersion string                   `json:"prompt_version,omitempty"`
}

type TranslationWorksetTermResult = companydata.TranslationTermResult

type TranslateBrregTranslationWorksetBatchResult struct {
	Results []TranslationWorksetTermResult `json:"results"`
}

type SaveBrregTranslationWorksetBatchInput struct {
	Path          string                         `json:"path"`
	BatchID       int64                          `json:"batch_id"`
	Provider      string                         `json:"provider,omitempty"`
	Model         string                         `json:"model,omitempty"`
	PromptVersion string                         `json:"prompt_version,omitempty"`
	Results       []TranslationWorksetTermResult `json:"results"`
}

type SaveBrregTranslationWorksetBatchResult = companydata.SaveTranslationWorksetBatchResult

type ApplyBrregTranslationWorksetInput struct {
	Path          string `json:"path"`
	PromptVersion string `json:"prompt_version,omitempty"`
}

type ApplyBrregTranslationWorksetResult = companydata.ApplyTranslationWorksetResult

func (a *CompanyTranslationActions) BuildBrregTranslationWorkset(
	ctx context.Context,
	input BuildBrregTranslationWorksetInput,
) (BuildBrregTranslationWorksetResult, error) {
	if a == nil || a.store == nil {
		return BuildBrregTranslationWorksetResult{}, errors.New("brreg companydata store not available")
	}
	slog.DebugContext(ctx, "building brreg translation workset",
		"path", input.Path,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"company_limit", input.CompanyLimit,
		"field_limit", input.FieldLimit,
		"prompt_version", input.PromptVersion,
	)
	result, err := a.store.BuildTranslationWorkset(ctx, companydata.BuildTranslationWorksetCommand{
		Path:          input.Path,
		PromptVersion: input.PromptVersion,
		IDs:           input.IDs,
		Filters:       input.Filters,
		CompanyLimit:  input.CompanyLimit,
		FieldLimit:    input.FieldLimit,
	})
	if err != nil {
		return BuildBrregTranslationWorksetResult{}, errors.Wrap(err, "build brreg translation workset")
	}
	slog.DebugContext(ctx, "built brreg translation workset",
		"path", result.Path,
		"fields_exported", result.FieldsExported,
		"terms_exported", result.TermsExported,
		"companies_exported", result.CompaniesExported,
		"cached_fields", result.CachedFields,
	)
	return result, nil
}

func (a *CompanyTranslationActions) ClaimBrregTranslationWorksetBatch(
	ctx context.Context,
	input ClaimBrregTranslationWorksetBatchInput,
) (ClaimBrregTranslationWorksetBatchResult, error) {
	result, err := companydata.ClaimTranslationWorksetBatch(ctx, companydata.ClaimTranslationWorksetBatchCommand{
		Path:            input.Path,
		MaxRequestChars: input.MaxRequestChars,
		MaxTerms:        input.MaxTerms,
		MaxAttempts:     input.MaxAttempts,
	})
	if err != nil {
		return ClaimBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "claim brreg translation workset batch")
	}
	slog.DebugContext(ctx, "claimed brreg translation workset batch",
		"path", input.Path,
		"status", result.Status,
		"batch_id", result.BatchID,
		"terms", len(result.Terms),
		"estimated_chars", result.EstimatedChars,
	)
	return result, nil
}

func (a *CompanyTranslationActions) TranslateBrregTranslationWorksetBatch(
	ctx context.Context,
	input TranslateBrregTranslationWorksetBatchInput,
) (TranslateBrregTranslationWorksetBatchResult, error) {
	if a == nil || a.translator == nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.New("brreg term translation client not available")
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultTranslationPromptVersion
	}
	if input.Provider == "" {
		input.Provider = "default"
	}
	request := translationclient.TermTranslationRequest{
		RequestID:     uuid.NewString(),
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Terms:         make([]translationclient.TermTranslationRequestTerm, 0, len(input.Terms)),
	}
	for _, term := range input.Terms {
		request.Terms = append(request.Terms, translationclient.TermTranslationRequestTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
		})
	}
	slog.DebugContext(ctx, "translating brreg translation workset batch",
		"batch_id", input.BatchID,
		"terms", len(request.Terms),
		"provider", request.Provider,
		"model", request.Model,
		"prompt_version", request.PromptVersion,
	)
	response, err := a.translator.TranslateBrregTerms(ctx, request)
	if err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "translate brreg translation workset batch")
	}
	result := TranslateBrregTranslationWorksetBatchResult{
		Results: make([]TranslationWorksetTermResult, 0, len(response.Results)+len(response.Failures)),
	}
	for _, item := range response.Results {
		result.Results = append(result.Results, companydata.TranslationTermResult{
			TermKey:              item.TermKey,
			SourceText:           item.SourceText,
			SourceTextNormalized: item.SourceTextNormalized,
			TranslatedText:       item.TranslatedText,
			Status:               defaultString(item.Status, "succeeded"),
			Provider:             defaultString(response.Provider, input.Provider),
			Model:                defaultString(response.Model, input.Model),
			PromptVersion:        defaultString(response.PromptVersion, input.PromptVersion),
		})
	}
	for _, failure := range response.Failures {
		result.Results = append(result.Results, companydata.TranslationTermResult{
			TermKey:              failure.TermKey,
			SourceText:           failure.SourceText,
			SourceTextNormalized: failure.SourceTextNormalized,
			Status:               defaultString(failure.Status, "failed_retryable"),
			Provider:             defaultString(response.Provider, input.Provider),
			Model:                defaultString(response.Model, input.Model),
			PromptVersion:        defaultString(response.PromptVersion, input.PromptVersion),
			Error:                failure.Error,
			ErrorCode:            failure.ErrorCode,
		})
	}
	return result, nil
}

func (a *CompanyTranslationActions) SaveBrregTranslationWorksetBatch(
	ctx context.Context,
	input SaveBrregTranslationWorksetBatchInput,
) (SaveBrregTranslationWorksetBatchResult, error) {
	result, err := companydata.SaveTranslationWorksetBatch(ctx, companydata.SaveTranslationWorksetBatchCommand{
		Path:          input.Path,
		BatchID:       input.BatchID,
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Results:       input.Results,
	})
	if err != nil {
		return SaveBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "save brreg translation workset batch")
	}
	slog.DebugContext(ctx, "saved brreg translation workset batch",
		"path", input.Path,
		"batch_id", input.BatchID,
		"terms_succeeded", result.TermsSucceeded,
		"terms_failed", result.TermsFailed,
	)
	return result, nil
}

func (a *CompanyTranslationActions) ApplyBrregTranslationWorkset(
	ctx context.Context,
	input ApplyBrregTranslationWorksetInput,
) (ApplyBrregTranslationWorksetResult, error) {
	if a == nil || a.store == nil {
		return ApplyBrregTranslationWorksetResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.ApplyTranslationWorkset(ctx, companydata.ApplyTranslationWorksetCommand{
		Path:          input.Path,
		PromptVersion: input.PromptVersion,
	})
	if err != nil {
		return ApplyBrregTranslationWorksetResult{}, errors.Wrap(err, "apply brreg translation workset")
	}
	slog.DebugContext(ctx, "applied brreg translation workset",
		"path", input.Path,
		"terms_saved", result.TermsSaved,
		"bindings_applied", result.BindingsApplied,
	)
	return result, nil
}
