package actions

import (
	"context"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
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

type BuildBrregTranslationWorksetResult struct {
	Path                string `json:"path,omitempty"`
	FieldsExported      int32  `json:"fields_exported"`
	TermsExported       int32  `json:"terms_exported"`
	CompaniesExported   int32  `json:"companies_exported"`
	CachedFields        int32  `json:"cached_fields"`
	CompaniesQueued     int32  `json:"companies_queued"`
	TerminalRowsDeleted int32  `json:"terminal_rows_deleted"`
}

type ClaimBrregTranslationWorksetBatchInput struct {
	Path                string `json:"path,omitempty"`
	BatchID             string `json:"batch_id,omitempty"`
	MaxRequestChars     int32  `json:"max_request_chars,omitempty"`
	MaxTerms            int32  `json:"max_terms,omitempty"`
	MaxAttempts         int32  `json:"max_attempts,omitempty"`
	StaleRunningSeconds int32  `json:"stale_running_seconds,omitempty"`
}

type TranslationWorksetTerm = companydata.TranslationWorksetTerm
type ClaimBrregTranslationWorksetBatchResult = companydata.ClaimTranslationQueueBatchResult

type TranslateBrregTranslationWorksetBatchInput struct {
	BatchID       string   `json:"batch_id"`
	CompanyIDs    []string `json:"company_ids"`
	Provider      string   `json:"provider,omitempty"`
	Model         string   `json:"model,omitempty"`
	PromptVersion string   `json:"prompt_version,omitempty"`
}

type TranslationWorksetTermResult = companydata.TranslationTermResult

type TranslateBrregTranslationWorksetBatchResult struct {
	Results               []TranslationWorksetTermResult `json:"results"`
	CompaniesProcessed    int32                          `json:"companies_processed"`
	FieldsSeen            int32                          `json:"fields_seen"`
	TermsClaimed          int32                          `json:"terms_claimed"`
	TermsSucceeded        int32                          `json:"terms_succeeded"`
	TermsFailed           int32                          `json:"terms_failed"`
	TermsSaved            int32                          `json:"terms_saved"`
	BindingsApplied       int32                          `json:"bindings_applied"`
	CachedBindingsApplied int32                          `json:"cached_bindings_applied"`
}

type CompleteBrregTranslationQueueBatchInput struct {
	BatchID string `json:"batch_id"`
}

type ReleaseBrregTranslationQueueBatchInput struct {
	BatchID string `json:"batch_id"`
}

type TranslationQueueBatchResult = companydata.TranslationQueueBatchResult

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
	prepared, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
		IDs:          input.IDs,
		Filters:      input.Filters,
		CompanyLimit: input.CompanyLimit,
	})
	if err != nil {
		return BuildBrregTranslationWorksetResult{}, errors.Wrap(err, "prepare brreg translation queue")
	}
	result := BuildBrregTranslationWorksetResult{
		Path:                input.Path,
		FieldsExported:      prepared.FieldsSeen,
		TermsExported:       prepared.FieldsSeen,
		CompaniesExported:   prepared.CompaniesSeen,
		CompaniesQueued:     prepared.CompaniesQueued,
		TerminalRowsDeleted: prepared.TerminalRowsDeleted,
	}
	slog.DebugContext(ctx, "built brreg translation workset",
		"path", result.Path,
		"fields_exported", result.FieldsExported,
		"terms_exported", result.TermsExported,
		"companies_exported", result.CompaniesExported,
		"companies_queued", result.CompaniesQueued,
		"terminal_rows_deleted", result.TerminalRowsDeleted,
	)
	return result, nil
}

func (a *CompanyTranslationActions) ClaimBrregTranslationWorksetBatch(
	ctx context.Context,
	input ClaimBrregTranslationWorksetBatchInput,
) (ClaimBrregTranslationWorksetBatchResult, error) {
	if a == nil || a.store == nil {
		return ClaimBrregTranslationWorksetBatchResult{}, errors.New("brreg companydata store not available")
	}
	if _, err := a.store.ResetStaleTranslationQueueEntries(ctx, input.StaleRunningSeconds); err != nil {
		return ClaimBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "reset stale brreg translation queue entries")
	}
	result, err := a.store.ClaimTranslationQueueBatch(ctx, companydata.ClaimTranslationQueueBatchCommand{
		BatchID:          input.BatchID,
		MaxCandidateRows: input.MaxTerms,
		MaxRequestChars:  input.MaxRequestChars,
	})
	if err != nil {
		return ClaimBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "claim brreg translation queue batch")
	}
	slog.DebugContext(ctx, "claimed brreg translation workset batch",
		"path", input.Path,
		"status", result.Status,
		"batch_id", result.BatchID,
		"companies", len(result.CompanyIDs),
		"estimated_chars", result.EstimatedChars,
	)
	return result, nil
}

func (a *CompanyTranslationActions) TranslateBrregTranslationWorksetBatch(
	ctx context.Context,
	input TranslateBrregTranslationWorksetBatchInput,
) (TranslateBrregTranslationWorksetBatchResult, error) {
	if a == nil || a.store == nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.New("brreg companydata store not available")
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultTranslationPromptVersion
	}
	if input.Provider == "" {
		input.Provider = "default"
	}
	companyIDs := compactActionTextValues(input.CompanyIDs)
	if len(companyIDs) == 0 {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.New("brreg translation queue batch company ids are required")
	}
	fields, err := a.store.LoadMissingTranslationFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
		PromptVersion: input.PromptVersion,
		CompanyIDs:    companyIDs,
	})
	if err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "load brreg queue batch missing translation fields")
	}
	result := TranslateBrregTranslationWorksetBatchResult{
		CompaniesProcessed: int32(len(companyIDs)),
		FieldsSeen:         int32(len(fields)),
	}
	if len(fields) == 0 {
		if err := a.store.RefreshTranslationStatus(ctx); err != nil {
			return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "refresh brreg translation status after empty queue batch")
		}
		return result, nil
	}
	termKeys := sourcetranslation.TranslationTermKeys(fields)
	cachedTerms, err := a.store.LoadCachedTranslationTerms(ctx, sourcetranslation.LoadCachedTermsCommand{
		PromptVersion: input.PromptVersion,
		TermKeys:      termKeys,
	})
	if err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "load cached brreg queue batch translation terms")
	}
	builtTerms := sourcetranslation.BuildTranslationQueueTerms(fields, cachedTerms)
	result.TermsClaimed = int32(len(builtTerms.UncachedTerms))
	if len(builtTerms.UncachedTerms) > 0 && a.translator == nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.New("brreg term translation client not available")
	}
	if len(builtTerms.UncachedTerms) > 0 {
		translated, err := a.translateUncachedBrregQueueTerms(ctx, input, builtTerms.UncachedTerms)
		if err != nil {
			return TranslateBrregTranslationWorksetBatchResult{}, err
		}
		result.Results = translated
		result.TermsSucceeded, result.TermsFailed = countTranslationTermResults(translated)
		saved, err := a.store.SaveTranslationTerms(ctx, sourcetranslation.SaveTermsCommand{
			PromptVersion: input.PromptVersion,
			Terms:         translated,
		})
		if err != nil {
			return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "save brreg queue batch translation terms")
		}
		result.TermsSaved = saved.TermsSaved
	}
	resultBindings := sourcetranslation.BuildTranslationBindingsForResults(fields, result.Results)
	appliedCached, err := applyBrregQueueBindings(ctx, a.store, builtTerms.CachedBindings)
	if err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, err
	}
	result.CachedBindingsApplied = appliedCached
	appliedResults, err := applyBrregQueueBindings(ctx, a.store, resultBindings)
	if err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, err
	}
	result.BindingsApplied = appliedCached + appliedResults
	if err := a.store.RefreshTranslationStatus(ctx); err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "refresh brreg translation status after queue batch")
	}
	return result, nil
}

func (a *CompanyTranslationActions) translateUncachedBrregQueueTerms(
	ctx context.Context,
	input TranslateBrregTranslationWorksetBatchInput,
	terms []sourcetranslation.TranslationTerm,
) ([]TranslationWorksetTermResult, error) {
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
		return nil, errors.Wrap(err, "translate brreg translation queue batch")
	}
	results := make([]TranslationWorksetTermResult, 0, len(response.Results)+len(response.Failures))
	for _, item := range response.Results {
		results = append(results, companydata.TranslationTermResult{
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
		results = append(results, companydata.TranslationTermResult{
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
	return results, nil
}

func (a *CompanyTranslationActions) CompleteBrregTranslationQueueBatch(
	ctx context.Context,
	input CompleteBrregTranslationQueueBatchInput,
) (TranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return TranslationQueueBatchResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.CompleteTranslationQueueBatch(ctx, input.BatchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "complete brreg translation queue batch")
	}
	return result, nil
}

func (a *CompanyTranslationActions) ReleaseBrregTranslationQueueBatch(
	ctx context.Context,
	input ReleaseBrregTranslationQueueBatchInput,
) (TranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return TranslationQueueBatchResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.ReleaseTranslationQueueBatch(ctx, input.BatchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "release brreg translation queue batch")
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

func applyBrregQueueBindings(
	ctx context.Context,
	store *companydata.Store,
	bindings []sourcetranslation.TranslationBinding,
) (int32, error) {
	if len(bindings) == 0 {
		return 0, nil
	}
	grouped := groupBrregQueueBindingsByCompany(bindings)
	var applied int32
	for _, group := range grouped {
		result, err := store.ApplyCompanyTranslations(ctx, sourcetranslation.ApplyCompanyTranslationsCommand{
			CompanyID: group.CompanyID,
			Bindings:  group.Bindings,
		})
		if err != nil {
			return 0, errors.Wrapf(err, "apply brreg queue translation bindings for company %s", group.CompanyID)
		}
		applied += result.BindingsApplied
	}
	return applied, nil
}

type brregQueueBindingGroup struct {
	CompanyID string
	Bindings  []sourcetranslation.TranslationBinding
}

func groupBrregQueueBindingsByCompany(
	bindings []sourcetranslation.TranslationBinding,
) []brregQueueBindingGroup {
	groups := make([]brregQueueBindingGroup, 0)
	indexByCompany := make(map[string]int)
	for _, binding := range bindings {
		companyID := strings.TrimSpace(binding.CompanyID)
		if companyID == "" {
			continue
		}
		index, ok := indexByCompany[companyID]
		if !ok {
			index = len(groups)
			indexByCompany[companyID] = index
			groups = append(groups, brregQueueBindingGroup{CompanyID: companyID})
		}
		groups[index].Bindings = append(groups[index].Bindings, binding)
	}
	return groups
}

func countTranslationTermResults(results []TranslationWorksetTermResult) (succeeded int32, failed int32) {
	for _, result := range results {
		switch result.Status {
		case "succeeded":
			succeeded++
		default:
			failed++
		}
	}
	return succeeded, failed
}

func compactActionTextValues(values []string) []string {
	compact := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			compact = append(compact, value)
		}
	}
	return compact
}
