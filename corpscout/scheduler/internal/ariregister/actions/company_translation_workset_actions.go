package actions

import (
	"context"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationqueue"
)

type BuildAriregisterTranslationWorksetInput struct {
	Path          string            `json:"path"`
	Provider      string            `json:"provider,omitempty"`
	Model         string            `json:"model,omitempty"`
	PromptVersion string            `json:"prompt_version,omitempty"`
	IDs           []string          `json:"ids,omitempty"`
	Filters       map[string]string `json:"filters,omitempty"`
	CompanyLimit  int32             `json:"company_limit,omitempty"`
	FieldLimit    int32             `json:"field_limit,omitempty"`
}

type BuildAriregisterTranslationWorksetResult struct {
	Path                string `json:"path,omitempty"`
	FieldsExported      int32  `json:"fields_exported"`
	TermsExported       int32  `json:"terms_exported"`
	CompaniesExported   int32  `json:"companies_exported"`
	CachedFields        int32  `json:"cached_fields"`
	CompaniesQueued     int32  `json:"companies_queued"`
	TerminalRowsDeleted int32  `json:"terminal_rows_deleted"`
}

type ClaimAriregisterTranslationWorksetBatchInput struct {
	Path                string `json:"path,omitempty"`
	BatchID             string `json:"batch_id,omitempty"`
	MaxRequestChars     int32  `json:"max_request_chars,omitempty"`
	MaxTerms            int32  `json:"max_terms,omitempty"`
	MaxSourceRunning    int32  `json:"max_source_running,omitempty"`
	MaxGlobalRunning    int32  `json:"max_global_running,omitempty"`
	MaxAttempts         int32  `json:"max_attempts,omitempty"`
	StaleRunningSeconds int32  `json:"stale_running_seconds,omitempty"`
}

type TranslationWorksetTerm = companydata.TranslationWorksetTerm
type ClaimAriregisterTranslationWorksetBatchResult = companydata.ClaimTranslationQueueBatchResult

type TranslateAriregisterTranslationWorksetBatchInput struct {
	BatchID       string   `json:"batch_id"`
	CompanyIDs    []string `json:"company_ids"`
	SourceLang    string   `json:"source_lang,omitempty"`
	TargetLang    string   `json:"target_lang,omitempty"`
	Provider      string   `json:"provider,omitempty"`
	Model         string   `json:"model,omitempty"`
	PromptVersion string   `json:"prompt_version,omitempty"`
}

type TranslationWorksetTermResult = companydata.TranslationTermResult

type TranslateAriregisterTranslationWorksetBatchResult struct {
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

type CompleteAriregisterTranslationQueueBatchInput struct {
	BatchID string `json:"batch_id"`
}

type ReleaseAriregisterTranslationQueueBatchInput struct {
	BatchID string `json:"batch_id"`
}

type TranslationQueueBatchResult = companydata.TranslationQueueBatchResult

type SaveAriregisterTranslationWorksetBatchInput struct {
	Path          string                         `json:"path"`
	BatchID       int64                          `json:"batch_id"`
	Provider      string                         `json:"provider,omitempty"`
	Model         string                         `json:"model,omitempty"`
	PromptVersion string                         `json:"prompt_version,omitempty"`
	Results       []TranslationWorksetTermResult `json:"results"`
}

type SaveAriregisterTranslationWorksetBatchResult = companydata.SaveTranslationWorksetBatchResult

type ApplyAriregisterTranslationWorksetInput struct {
	Path          string `json:"path"`
	PromptVersion string `json:"prompt_version,omitempty"`
}

type ApplyAriregisterTranslationWorksetResult = companydata.ApplyTranslationWorksetResult

var _ translationqueue.SourceQueue = (*CompanyTranslationActions)(nil)

func (a *CompanyTranslationActions) Name() string {
	return "ariregister"
}

func (a *CompanyTranslationActions) PrepareQueue(
	ctx context.Context,
	command translationqueue.PrepareQueueCommand,
) error {
	if a == nil || a.store == nil {
		return errors.New("ariregister companydata store not available")
	}
	_, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
		IDs:           command.IDs,
		Filters:       command.Filters,
		CompanyLimit:  command.CompanyLimit,
		Provider:      command.Provider,
		Model:         command.Model,
		PromptVersion: command.PromptVersion,
		SourceLang:    command.SourceLang,
		TargetLang:    command.TargetLang,
	})
	if err != nil {
		return errors.Wrap(err, "prepare ariregister translation queue")
	}
	return nil
}

func (a *CompanyTranslationActions) ClaimBatch(
	ctx context.Context,
	command translationqueue.ClaimBatchCommand,
) (translationqueue.ClaimBatchResult, error) {
	if a == nil || a.store == nil {
		return translationqueue.ClaimBatchResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.ClaimTranslationQueueBatch(ctx, companydata.ClaimTranslationQueueBatchCommand{
		BatchID:          command.BatchID,
		MaxCandidateRows: command.MaxCandidateRows,
		MaxRequestChars:  command.MaxRequestChars,
		MaxSourceRunning: command.MaxSourceRunning,
	})
	if err != nil {
		return translationqueue.ClaimBatchResult{}, errors.Wrap(err, "claim ariregister translation queue batch")
	}
	return translationqueue.ClaimBatchResult{
		Status:         result.Status,
		BatchID:        result.BatchID,
		CompanyIDs:     result.CompanyIDs,
		EstimatedChars: result.EstimatedChars,
		Provider:       result.Provider,
		Model:          result.Model,
		PromptVersion:  result.PromptVersion,
		SourceLang:     result.SourceLang,
		TargetLang:     result.TargetLang,
	}, nil
}

func (a *CompanyTranslationActions) ReleaseBatch(
	ctx context.Context,
	batchID string,
) (translationqueue.QueueBatchResult, error) {
	if a == nil || a.store == nil {
		return translationqueue.QueueBatchResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.ReleaseTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return translationqueue.QueueBatchResult{}, errors.Wrap(err, "release ariregister translation queue batch")
	}
	return translationqueue.QueueBatchResult{RowsAffected: result.RowsAffected}, nil
}

func (a *CompanyTranslationActions) CompleteBatch(
	ctx context.Context,
	batchID string,
) (translationqueue.QueueBatchResult, error) {
	if a == nil || a.store == nil {
		return translationqueue.QueueBatchResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.CompleteTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return translationqueue.QueueBatchResult{}, errors.Wrap(err, "complete ariregister translation queue batch")
	}
	return translationqueue.QueueBatchResult{RowsAffected: result.RowsAffected}, nil
}

func (a *CompanyTranslationActions) ResetStale(
	ctx context.Context,
	staleRunningSeconds int32,
) (translationqueue.QueueBatchResult, error) {
	if a == nil || a.store == nil {
		return translationqueue.QueueBatchResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.ResetStaleTranslationQueueEntries(ctx, staleRunningSeconds)
	if err != nil {
		return translationqueue.QueueBatchResult{}, errors.Wrap(err, "reset stale ariregister translation queue entries")
	}
	return translationqueue.QueueBatchResult{RowsAffected: result.RowsAffected}, nil
}

func (a *CompanyTranslationActions) LoadMissingFields(
	ctx context.Context,
	command sourcetranslation.LoadMissingFieldsCommand,
) ([]sourcetranslation.MissingField, error) {
	if a == nil || a.store == nil {
		return nil, errors.New("ariregister companydata store not available")
	}
	fields, err := a.store.LoadMissingTranslationFields(ctx, command)
	if err != nil {
		return nil, errors.Wrap(err, "load ariregister missing translation fields")
	}
	return fields, nil
}

func (a *CompanyTranslationActions) LoadCachedTerms(
	ctx context.Context,
	command sourcetranslation.LoadCachedTermsCommand,
) (map[string]sourcetranslation.CachedTerm, error) {
	if a == nil || a.store == nil {
		return nil, errors.New("ariregister companydata store not available")
	}
	terms, err := a.store.LoadCachedTranslationTerms(ctx, command)
	if err != nil {
		return nil, errors.Wrap(err, "load cached ariregister translation terms")
	}
	return terms, nil
}

func (a *CompanyTranslationActions) SaveTerms(
	ctx context.Context,
	command sourcetranslation.SaveTermsCommand,
) (sourcetranslation.SaveTermsResult, error) {
	if a == nil || a.store == nil {
		return sourcetranslation.SaveTermsResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.SaveTranslationTerms(ctx, command)
	if err != nil {
		return sourcetranslation.SaveTermsResult{}, errors.Wrap(err, "save ariregister translation terms")
	}
	return result, nil
}

func (a *CompanyTranslationActions) ApplyTranslations(
	ctx context.Context,
	command sourcetranslation.ApplyCompanyTranslationsCommand,
) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	if a == nil || a.store == nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.ApplyCompanyTranslations(ctx, command)
	if err != nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "apply ariregister company translations")
	}
	return result, nil
}

func (a *CompanyTranslationActions) BuildAriregisterTranslationWorkset(
	ctx context.Context,
	input BuildAriregisterTranslationWorksetInput,
) (BuildAriregisterTranslationWorksetResult, error) {
	if a == nil || a.store == nil {
		return BuildAriregisterTranslationWorksetResult{}, errors.New("ariregister companydata store not available")
	}
	slog.DebugContext(ctx, "building ariregister translation workset",
		"path", input.Path,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"company_limit", input.CompanyLimit,
		"field_limit", input.FieldLimit,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
	)
	prepared, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
		IDs:           input.IDs,
		Filters:       input.Filters,
		CompanyLimit:  input.CompanyLimit,
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
	})
	if err != nil {
		return BuildAriregisterTranslationWorksetResult{}, errors.Wrap(err, "prepare ariregister translation queue")
	}
	result := BuildAriregisterTranslationWorksetResult{
		Path:                input.Path,
		FieldsExported:      prepared.FieldsSeen,
		TermsExported:       prepared.FieldsSeen,
		CompaniesExported:   prepared.CompaniesSeen,
		CompaniesQueued:     prepared.CompaniesQueued,
		TerminalRowsDeleted: prepared.TerminalRowsDeleted,
	}
	slog.DebugContext(ctx, "built ariregister translation workset",
		"path", result.Path,
		"fields_exported", result.FieldsExported,
		"terms_exported", result.TermsExported,
		"companies_exported", result.CompaniesExported,
		"companies_queued", result.CompaniesQueued,
		"terminal_rows_deleted", result.TerminalRowsDeleted,
	)
	return result, nil
}

func (a *CompanyTranslationActions) ClaimAriregisterTranslationWorksetBatch(
	ctx context.Context,
	input ClaimAriregisterTranslationWorksetBatchInput,
) (ClaimAriregisterTranslationWorksetBatchResult, error) {
	if a == nil || a.store == nil {
		return ClaimAriregisterTranslationWorksetBatchResult{}, errors.New("ariregister companydata store not available")
	}
	if _, err := a.store.ResetStaleTranslationQueueEntries(ctx, input.StaleRunningSeconds); err != nil {
		return ClaimAriregisterTranslationWorksetBatchResult{}, errors.Wrap(err, "reset stale ariregister translation queue entries")
	}
	result, err := a.store.ClaimTranslationQueueBatch(ctx, companydata.ClaimTranslationQueueBatchCommand{
		BatchID:          input.BatchID,
		MaxCandidateRows: input.MaxTerms,
		MaxRequestChars:  input.MaxRequestChars,
		MaxSourceRunning: effectiveMaxSourceRunning(input.MaxSourceRunning, input.MaxGlobalRunning),
	})
	if err != nil {
		return ClaimAriregisterTranslationWorksetBatchResult{}, errors.Wrap(err, "claim ariregister translation queue batch")
	}
	slog.DebugContext(ctx, "claimed ariregister translation workset batch",
		"path", input.Path,
		"status", result.Status,
		"batch_id", result.BatchID,
		"companies", len(result.CompanyIDs),
		"estimated_chars", result.EstimatedChars,
		"max_source_running", effectiveMaxSourceRunning(input.MaxSourceRunning, input.MaxGlobalRunning),
	)
	return result, nil
}

func (a *CompanyTranslationActions) RefreshAriregisterTranslationStatus(ctx context.Context) error {
	if a == nil || a.store == nil {
		return errors.New("ariregister companydata store not available")
	}
	if err := a.store.RefreshTranslationStatus(ctx); err != nil {
		return errors.Wrap(err, "refresh ariregister translation status")
	}
	return nil
}

func (a *CompanyTranslationActions) TranslateAriregisterTranslationWorksetBatch(
	ctx context.Context,
	input TranslateAriregisterTranslationWorksetBatchInput,
) (TranslateAriregisterTranslationWorksetBatchResult, error) {
	if a == nil || a.store == nil {
		return TranslateAriregisterTranslationWorksetBatchResult{}, errors.New("ariregister companydata store not available")
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultTranslationPromptVersion
	}
	if input.Provider == "" {
		input.Provider = "default"
	}
	if input.SourceLang == "" {
		input.SourceLang = "et"
	}
	if input.TargetLang == "" {
		input.TargetLang = "en"
	}
	companyIDs := compactActionTextValues(input.CompanyIDs)
	if len(companyIDs) == 0 {
		return TranslateAriregisterTranslationWorksetBatchResult{}, errors.New("ariregister translation queue batch company ids are required")
	}
	fields, err := a.store.LoadMissingTranslationFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
		PromptVersion: input.PromptVersion,
		CompanyIDs:    companyIDs,
	})
	if err != nil {
		return TranslateAriregisterTranslationWorksetBatchResult{}, errors.Wrap(err, "load ariregister queue batch missing translation fields")
	}
	result := TranslateAriregisterTranslationWorksetBatchResult{
		CompaniesProcessed: int32(len(companyIDs)),
		FieldsSeen:         int32(len(fields)),
	}
	if len(fields) == 0 {
		return result, nil
	}
	termKeys := sourcetranslation.TranslationTermKeys(fields)
	cachedTerms, err := a.store.LoadCachedTranslationTerms(ctx, sourcetranslation.LoadCachedTermsCommand{
		PromptVersion: input.PromptVersion,
		SourceLang:    input.SourceLang,
		TargetLang:    input.TargetLang,
		TermKeys:      termKeys,
	})
	if err != nil {
		return TranslateAriregisterTranslationWorksetBatchResult{}, errors.Wrap(err, "load cached ariregister queue batch translation terms")
	}
	builtTerms := sourcetranslation.BuildTranslationQueueTerms(fields, cachedTerms)
	result.TermsClaimed = int32(len(builtTerms.UncachedTerms))
	if len(builtTerms.UncachedTerms) > 0 && a.translator == nil {
		return TranslateAriregisterTranslationWorksetBatchResult{}, errors.New("ariregister term translation client not available")
	}
	if len(builtTerms.UncachedTerms) > 0 {
		translated, err := a.translateUncachedAriregisterQueueTerms(ctx, input, builtTerms.UncachedTerms)
		if err != nil {
			return TranslateAriregisterTranslationWorksetBatchResult{}, err
		}
		result.Results = translated
		result.TermsSucceeded, result.TermsFailed = countTranslationTermResults(translated)
		saved, err := a.store.SaveTranslationTerms(ctx, sourcetranslation.SaveTermsCommand{
			PromptVersion: input.PromptVersion,
			SourceLang:    input.SourceLang,
			TargetLang:    input.TargetLang,
			Terms:         translated,
		})
		if err != nil {
			return TranslateAriregisterTranslationWorksetBatchResult{}, errors.Wrap(err, "save ariregister queue batch translation terms")
		}
		result.TermsSaved = saved.TermsSaved
	}
	resultBindings := sourcetranslation.BuildTranslationBindingsForResults(fields, result.Results)
	appliedCached, err := applyAriregisterQueueBindings(ctx, a.store, builtTerms.CachedBindings)
	if err != nil {
		return TranslateAriregisterTranslationWorksetBatchResult{}, err
	}
	result.CachedBindingsApplied = appliedCached
	appliedResults, err := applyAriregisterQueueBindings(ctx, a.store, resultBindings)
	if err != nil {
		return TranslateAriregisterTranslationWorksetBatchResult{}, err
	}
	result.BindingsApplied = appliedCached + appliedResults
	return result, nil
}

func (a *CompanyTranslationActions) translateUncachedAriregisterQueueTerms(
	ctx context.Context,
	input TranslateAriregisterTranslationWorksetBatchInput,
	terms []sourcetranslation.TranslationTerm,
) ([]TranslationWorksetTermResult, error) {
	request := translationclient.TermTranslationRequest{
		RequestID:     uuid.NewString(),
		Source:        "ariregister",
		SourceLang:    input.SourceLang,
		TargetLang:    input.TargetLang,
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
	slog.DebugContext(ctx, "translating ariregister translation workset batch",
		"batch_id", input.BatchID,
		"terms", len(request.Terms),
		"provider", request.Provider,
		"model", request.Model,
		"source_lang", request.SourceLang,
		"target_lang", request.TargetLang,
		"prompt_version", request.PromptVersion,
	)
	response, err := a.translator.TranslateTerms(ctx, request)
	if err != nil {
		return nil, errors.Wrap(err, "translate ariregister translation queue batch")
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

func (a *CompanyTranslationActions) CompleteAriregisterTranslationQueueBatch(
	ctx context.Context,
	input CompleteAriregisterTranslationQueueBatchInput,
) (TranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return TranslationQueueBatchResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.CompleteTranslationQueueBatch(ctx, input.BatchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "complete ariregister translation queue batch")
	}
	return result, nil
}

func (a *CompanyTranslationActions) ReleaseAriregisterTranslationQueueBatch(
	ctx context.Context,
	input ReleaseAriregisterTranslationQueueBatchInput,
) (TranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return TranslationQueueBatchResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.ReleaseTranslationQueueBatch(ctx, input.BatchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "release ariregister translation queue batch")
	}
	return result, nil
}

func (a *CompanyTranslationActions) SaveAriregisterTranslationWorksetBatch(
	ctx context.Context,
	input SaveAriregisterTranslationWorksetBatchInput,
) (SaveAriregisterTranslationWorksetBatchResult, error) {
	result, err := companydata.SaveTranslationWorksetBatch(ctx, companydata.SaveTranslationWorksetBatchCommand{
		Path:          input.Path,
		BatchID:       input.BatchID,
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Results:       input.Results,
	})
	if err != nil {
		return SaveAriregisterTranslationWorksetBatchResult{}, errors.Wrap(err, "save ariregister translation workset batch")
	}
	slog.DebugContext(ctx, "saved ariregister translation workset batch",
		"path", input.Path,
		"batch_id", input.BatchID,
		"terms_succeeded", result.TermsSucceeded,
		"terms_failed", result.TermsFailed,
	)
	return result, nil
}

func (a *CompanyTranslationActions) ApplyAriregisterTranslationWorkset(
	ctx context.Context,
	input ApplyAriregisterTranslationWorksetInput,
) (ApplyAriregisterTranslationWorksetResult, error) {
	if a == nil || a.store == nil {
		return ApplyAriregisterTranslationWorksetResult{}, errors.New("ariregister companydata store not available")
	}
	result, err := a.store.ApplyTranslationWorkset(ctx, companydata.ApplyTranslationWorksetCommand{
		Path:          input.Path,
		PromptVersion: input.PromptVersion,
	})
	if err != nil {
		return ApplyAriregisterTranslationWorksetResult{}, errors.Wrap(err, "apply ariregister translation workset")
	}
	slog.DebugContext(ctx, "applied ariregister translation workset",
		"path", input.Path,
		"terms_saved", result.TermsSaved,
		"bindings_applied", result.BindingsApplied,
	)
	return result, nil
}

func applyAriregisterQueueBindings(
	ctx context.Context,
	store *companydata.Store,
	bindings []sourcetranslation.TranslationBinding,
) (int32, error) {
	if len(bindings) == 0 {
		return 0, nil
	}
	grouped := groupAriregisterQueueBindingsByCompany(bindings)
	var applied int32
	for _, group := range grouped {
		result, err := store.ApplyCompanyTranslations(ctx, sourcetranslation.ApplyCompanyTranslationsCommand{
			CompanyID: group.CompanyID,
			Bindings:  group.Bindings,
		})
		if err != nil {
			return 0, errors.Wrapf(err, "apply ariregister queue translation bindings for company %s", group.CompanyID)
		}
		applied += result.BindingsApplied
	}
	return applied, nil
}

type ariregisterQueueBindingGroup struct {
	CompanyID string
	Bindings  []sourcetranslation.TranslationBinding
}

func groupAriregisterQueueBindingsByCompany(
	bindings []sourcetranslation.TranslationBinding,
) []ariregisterQueueBindingGroup {
	groups := make([]ariregisterQueueBindingGroup, 0)
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
			groups = append(groups, ariregisterQueueBindingGroup{CompanyID: companyID})
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

func effectiveMaxSourceRunning(maxSourceRunning int32, legacyMaxGlobalRunning int32) int32 {
	if maxSourceRunning > 0 {
		return maxSourceRunning
	}
	return legacyMaxGlobalRunning
}
