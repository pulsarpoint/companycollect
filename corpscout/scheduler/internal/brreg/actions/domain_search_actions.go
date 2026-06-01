package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
)

const (
	defaultDomainSearchLimit              = 1000
	defaultDomainSearchBatchSize          = 10
	defaultDomainSearchMaxAttempts        = 3
	defaultDomainSearchEngine             = "duckduckgo"
	defaultDomainSearchCountry            = "NO"
	defaultDomainSearchCandidateThreshold = 50
	defaultDomainSearchMaxCandidates      = 10
	defaultDomainSearchTimeoutSeconds     = 120
)

type DomainSearchActions struct {
	gateway      *brregdb.Gateway
	crawler      *crawlclient.Client
	llmProviders *llmproviders.Store
}

func NewDomainSearchActions(gateway *brregdb.Gateway, crawler *crawlclient.Client, llmProviders *llmproviders.Store) *DomainSearchActions {
	return &DomainSearchActions{gateway: gateway, crawler: crawler, llmProviders: llmProviders}
}

type PrepareBrregDomainSearchWorkflowInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	BatchSize          int32             `json:"batch_size"`
	MaxAttempts        int32             `json:"max_attempts"`
	Trigger            string            `json:"trigger,omitempty"`
}

type PrepareBrregDomainSearchWorkflowResult struct {
	WorkflowRunID   string `json:"workflow_run_id"`
	SelectionHash   string `json:"selection_hash"`
	RecordsSelected int32  `json:"records_selected"`
	BatchSize       int32  `json:"batch_size"`
	MaxAttempts     int32  `json:"max_attempts"`
}

func (a *DomainSearchActions) PrepareBrregDomainSearchWorkflow(ctx context.Context, input PrepareBrregDomainSearchWorkflowInput) (PrepareBrregDomainSearchWorkflowResult, error) {
	if a == nil || a.gateway == nil {
		return PrepareBrregDomainSearchWorkflowResult{}, errors.New("brreg domain search gateway not available")
	}
	slog.DebugContext(ctx, "preparing brreg domain search workflow",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"max_attempts", input.MaxAttempts,
		"trigger", input.Trigger,
	)
	prepared, err := a.gateway.PrepareWorkflow(ctx, prepareDomainSearchWorkflowCommandFromInput(input))
	if err != nil {
		return PrepareBrregDomainSearchWorkflowResult{}, errors.Wrap(err, "prepare brreg domain search workflow")
	}
	slog.DebugContext(ctx, "prepared brreg domain search workflow",
		"workflow_run_id", prepared.WorkflowRunID.String(),
		"selection_hash", prepared.SelectionHash,
		"records_selected", prepared.RecordsSelected,
		"batch_size", prepared.BatchSize,
		"max_attempts", prepared.MaxAttempts,
	)
	return PrepareBrregDomainSearchWorkflowResult{
		WorkflowRunID:   prepared.WorkflowRunID.String(),
		SelectionHash:   prepared.SelectionHash,
		RecordsSelected: prepared.RecordsSelected,
		BatchSize:       prepared.BatchSize,
		MaxAttempts:     prepared.MaxAttempts,
	}, nil
}

func prepareDomainSearchWorkflowCommandFromInput(input PrepareBrregDomainSearchWorkflowInput) brregdb.PrepareWorkflowCommand {
	return brregdb.PrepareWorkflowCommand{
		Source:             "brreg",
		Action:             "discover_domains",
		TaskType:           brregdb.TaskTypeDiscoverDomains,
		Trigger:            input.Trigger,
		WorkflowID:         input.TemporalWorkflowID,
		IDs:                input.IDs,
		Filters:            input.Filters,
		Limit:              input.Limit,
		BatchSize:          input.BatchSize,
		MaxAttempts:        input.MaxAttempts,
		DefaultLimit:       defaultDomainSearchLimit,
		DefaultBatchSize:   defaultDomainSearchBatchSize,
		DefaultMaxAttempts: defaultDomainSearchMaxAttempts,
	}
}

type FinishBrregDomainSearchWorkflowInput struct {
	WorkflowRunID    string `json:"workflow_run_id"`
	Status           string `json:"status"`
	RecordsSeen      int32  `json:"records_seen"`
	RecordsCompleted int32  `json:"records_completed"`
	RecordsFailed    int32  `json:"records_failed"`
	Error            string `json:"error,omitempty"`
}

type FinishBrregDomainSearchWorkflowResult struct{}

func (a *DomainSearchActions) FinishBrregDomainSearchWorkflow(ctx context.Context, input FinishBrregDomainSearchWorkflowInput) (FinishBrregDomainSearchWorkflowResult, error) {
	if a == nil || a.gateway == nil {
		return FinishBrregDomainSearchWorkflowResult{}, errors.New("brreg domain search gateway not available")
	}
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return FinishBrregDomainSearchWorkflowResult{}, errors.Wrap(err, "parse brreg domain search workflow run id")
	}
	var workflowError *string
	if input.Error != "" {
		workflowError = &input.Error
	}
	slog.DebugContext(ctx, "finishing brreg domain search workflow",
		"workflow_run_id", input.WorkflowRunID,
		"status", input.Status,
		"records_seen", input.RecordsSeen,
		"records_completed", input.RecordsCompleted,
		"records_failed", input.RecordsFailed,
		"has_error", workflowError != nil,
	)
	if err := a.gateway.FinishWorkflowRun(ctx, brregdb.FinishWorkflowRunCommand{
		WorkflowRunID:    workflowRunID,
		Status:           brregdb.WorkflowRunStatus(input.Status),
		RecordsSeen:      input.RecordsSeen,
		RecordsCompleted: input.RecordsCompleted,
		RecordsFailed:    input.RecordsFailed,
		Error:            workflowError,
	}); err != nil {
		return FinishBrregDomainSearchWorkflowResult{}, errors.Wrap(err, "finish brreg domain search workflow")
	}
	return FinishBrregDomainSearchWorkflowResult{}, nil
}

type FailRunningBrregDomainSearchTasksForWorkflowInput struct {
	WorkflowRunID string `json:"workflow_run_id"`
	MaxAttempts   int32  `json:"max_attempts"`
	Error         string `json:"error"`
}

type FailRunningBrregDomainSearchTasksForWorkflowResult struct {
	FailedTasks int32 `json:"failed_tasks"`
}

func (a *DomainSearchActions) FailRunningBrregDomainSearchTasksForWorkflow(ctx context.Context, input FailRunningBrregDomainSearchTasksForWorkflowInput) (FailRunningBrregDomainSearchTasksForWorkflowResult, error) {
	if a == nil || a.gateway == nil {
		return FailRunningBrregDomainSearchTasksForWorkflowResult{}, errors.New("brreg domain search gateway not available")
	}
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return FailRunningBrregDomainSearchTasksForWorkflowResult{}, errors.Wrap(err, "parse brreg domain search workflow run id")
	}
	errorMessage := input.Error
	failedTasks, err := a.gateway.FailRunningTasksForWorkflowRun(ctx, brregdb.FinishWorkflowRunCommand{
		WorkflowRunID: workflowRunID,
		MaxAttempts:   input.MaxAttempts,
		Error:         &errorMessage,
	})
	if err != nil {
		return FailRunningBrregDomainSearchTasksForWorkflowResult{}, errors.Wrap(err, "fail running brreg domain search tasks for workflow")
	}
	return FailRunningBrregDomainSearchTasksForWorkflowResult{FailedTasks: failedTasks}, nil
}

type ClaimBrregDomainSearchBatchInput struct {
	WorkflowRunID    string          `json:"workflow_run_id"`
	SelectionHash    string          `json:"selection_hash"`
	BatchSize        int32           `json:"batch_size"`
	MaxParallelTasks int32           `json:"max_parallel_tasks"`
	LeaseSeconds     int32           `json:"lease_seconds"`
	MaxAttempts      int32           `json:"max_attempts"`
	WorkerID         string          `json:"worker_id,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
}

type ClaimBrregDomainSearchBatchResult struct {
	Records []ClaimedDomainSearchRecord `json:"records"`
}

type ClaimedDomainSearchRecord struct {
	RawRecordID        string          `json:"raw_record_id"`
	TaskAttemptID      string          `json:"task_attempt_id"`
	OrganizationNumber string          `json:"organization_number"`
	OrganizationName   string          `json:"organization_name,omitempty"`
	Website            string          `json:"website,omitempty"`
	RawPayload         json.RawMessage `json:"raw_payload"`
	Attempt            int32           `json:"attempt"`
}

func (a *DomainSearchActions) ClaimBrregDomainSearchBatch(ctx context.Context, input ClaimBrregDomainSearchBatchInput) (ClaimBrregDomainSearchBatchResult, error) {
	if a == nil || a.gateway == nil {
		return ClaimBrregDomainSearchBatchResult{}, errors.New("brreg domain search gateway not available")
	}
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return ClaimBrregDomainSearchBatchResult{}, errors.Wrap(err, "parse brreg domain search workflow run id")
	}
	slog.DebugContext(ctx, "claiming brreg domain search batch",
		"workflow_run_id", input.WorkflowRunID,
		"selection_hash", input.SelectionHash,
		"batch_size", input.BatchSize,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"worker_id", input.WorkerID,
	)
	rows, err := a.gateway.ClaimDomainBatch(ctx, claimDomainSearchCommandFromInput(input, workflowRunID))
	if err != nil {
		return ClaimBrregDomainSearchBatchResult{}, errors.Wrap(err, "claim brreg domain search batch")
	}
	slog.DebugContext(ctx, "claimed brreg domain search batch",
		"workflow_run_id", input.WorkflowRunID,
		"records_count", len(rows),
		"first_raw_record_id", firstClaimedDomainSearchRawRecordID(rows),
	)
	return ClaimBrregDomainSearchBatchResult{Records: claimedDomainSearchRecordsFromRows(rows)}, nil
}

func claimDomainSearchCommandFromInput(input ClaimBrregDomainSearchBatchInput, workflowRunID uuid.UUID) brregdb.ClaimTaskBatchCommand {
	command := brregdb.ClaimTaskBatchCommand{
		WorkflowRunID:    &workflowRunID,
		SelectionHash:    input.SelectionHash,
		BatchSize:        input.BatchSize,
		MaxParallelTasks: input.MaxParallelTasks,
		LeaseSeconds:     input.LeaseSeconds,
		MaxAttempts:      input.MaxAttempts,
		Metadata:         input.Metadata,
	}
	if input.WorkerID != "" {
		command.WorkerID = &input.WorkerID
	}
	return command
}

func claimedDomainSearchRecordsFromRows(rows []db.ClaimBrregWorkflowTaskSelectionBatchRow) []ClaimedDomainSearchRecord {
	records := make([]ClaimedDomainSearchRecord, 0, len(rows))
	for _, row := range rows {
		record := ClaimedDomainSearchRecord{
			RawRecordID:        row.RawRecordID.String(),
			TaskAttemptID:      row.TaskAttemptID.String(),
			OrganizationNumber: row.OrganizationNumber,
			RawPayload:         row.RawPayload,
			Attempt:            row.Attempt,
		}
		if row.OrganizationName != nil {
			record.OrganizationName = *row.OrganizationName
		}
		if row.Website != nil {
			record.Website = *row.Website
		}
		records = append(records, record)
	}
	return records
}

type FetchBrregDomainSearchPagesInput struct {
	Records        []ClaimedDomainSearchRecord `json:"records"`
	SearchEngine   string                      `json:"search_engine,omitempty"`
	TimeoutSeconds int                         `json:"timeout_seconds,omitempty"`
}

type FetchBrregDomainSearchPagesResult struct {
	Results []DomainSearchPageResult `json:"results"`
}

type DomainSearchPageResult struct {
	RawRecordID        string                        `json:"raw_record_id"`
	TaskAttemptID      string                        `json:"task_attempt_id"`
	OrganizationNumber string                        `json:"organization_number"`
	Attempt            int32                         `json:"attempt"`
	SearchEngine       string                        `json:"search_engine"`
	SearchTerm         string                        `json:"search_term"`
	TimeoutSeconds     int                           `json:"timeout_seconds"`
	Status             string                        `json:"status"`
	Search             *crawlclient.Crawl4AiResponse `json:"search,omitempty"`
	Error              *DomainSearchError            `json:"error,omitempty"`
}

func (a *DomainSearchActions) FetchBrregDomainSearchPages(ctx context.Context, input FetchBrregDomainSearchPagesInput) (FetchBrregDomainSearchPagesResult, error) {
	if a == nil || a.crawler == nil {
		return FetchBrregDomainSearchPagesResult{}, errors.New("brreg crawl client not available")
	}
	searchEngine := defaultString(input.SearchEngine, defaultDomainSearchEngine)
	timeoutSeconds := defaultPositive(input.TimeoutSeconds, defaultDomainSearchTimeoutSeconds)
	slog.DebugContext(ctx, "fetching brreg domain search pages",
		"records_count", len(input.Records),
		"search_engine", searchEngine,
		"timeout_seconds", timeoutSeconds,
	)
	results := make([]DomainSearchPageResult, 0, len(input.Records))
	for _, record := range input.Records {
		facts := domainFactsFromRecord(record)
		searchTerm := domainSearchTerm(facts)
		response, err := a.crawler.FetchSearchPage(ctx, crawlclient.SearchFetchRequest{
			SearchTerm:     searchTerm,
			SearchEngine:   searchEngine,
			TimeoutSeconds: timeoutSeconds,
		})
		result := DomainSearchPageResult{
			RawRecordID:        record.RawRecordID,
			TaskAttemptID:      record.TaskAttemptID,
			OrganizationNumber: record.OrganizationNumber,
			Attempt:            record.Attempt,
			SearchEngine:       searchEngine,
			SearchTerm:         searchTerm,
			TimeoutSeconds:     timeoutSeconds,
		}
		if err != nil {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = &DomainSearchError{
				Message:       "Search page fetch request failed.",
				Category:      "crawl_service",
				Code:          "search_fetch_request_failed",
				RetryStrategy: "retry_with_backoff",
				Detail:        map[string]any{"error": err.Error()},
			}
			results = append(results, result)
			continue
		}
		result.Search = &response
		if response.Status != "succeeded" {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = domainSearchErrorFromCrawlResponse(response, "domain_search_failed", "Search page fetch failed.")
			results = append(results, result)
			continue
		}
		result.Status = brregdb.ResultStatusSucceeded.String()
		results = append(results, result)
	}
	return FetchBrregDomainSearchPagesResult{Results: results}, nil
}

type AnalyzeBrregDomainSearchPagesInput struct {
	Records            []ClaimedDomainSearchRecord `json:"records"`
	Pages              []DomainSearchPageResult    `json:"pages"`
	Provider           string                      `json:"provider,omitempty"`
	Model              string                      `json:"model,omitempty"`
	CandidateThreshold int                         `json:"candidate_threshold,omitempty"`
	MaxCandidates      int                         `json:"max_candidates,omitempty"`
	TimeoutSeconds     int                         `json:"timeout_seconds,omitempty"`
}

type AnalyzeBrregDomainSearchPagesResult struct {
	Results []DomainSearchRecordResult `json:"results"`
}

type DomainSearchRecordResult struct {
	RawRecordID        string                        `json:"raw_record_id"`
	TaskAttemptID      string                        `json:"task_attempt_id"`
	OrganizationNumber string                        `json:"organization_number"`
	Attempt            int32                         `json:"attempt"`
	SearchEngine       string                        `json:"search_engine"`
	SearchTerm         string                        `json:"search_term"`
	CandidateThreshold int                           `json:"candidate_threshold"`
	MaxCandidates      int                           `json:"max_candidates"`
	TimeoutSeconds     int                           `json:"timeout_seconds"`
	Status             string                        `json:"status"`
	Search             *crawlclient.Crawl4AiResponse `json:"search,omitempty"`
	Candidates         []crawlclient.ScoredLink      `json:"candidates"`
	AnalysisAttempted  bool                          `json:"analysis_attempted"`
	Provider           string                        `json:"provider,omitempty"`
	Model              string                        `json:"model,omitempty"`
	Error              *DomainSearchError            `json:"error,omitempty"`
}

type DomainSearchError struct {
	Message       string         `json:"message"`
	Category      string         `json:"category,omitempty"`
	Code          string         `json:"code,omitempty"`
	RetryStrategy string         `json:"retry_strategy,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
}

func (a *DomainSearchActions) AnalyzeBrregDomainSearchPages(ctx context.Context, input AnalyzeBrregDomainSearchPagesInput) (AnalyzeBrregDomainSearchPagesResult, error) {
	if a == nil || a.crawler == nil {
		return AnalyzeBrregDomainSearchPagesResult{}, errors.New("brreg crawl client not available")
	}
	llm, err := a.domainSearchLLMSelection(ctx, input.Provider, input.Model)
	if err != nil {
		return AnalyzeBrregDomainSearchPagesResult{}, err
	}
	candidateThreshold := defaultPositive(input.CandidateThreshold, defaultDomainSearchCandidateThreshold)
	maxCandidates := defaultPositive(input.MaxCandidates, defaultDomainSearchMaxCandidates)
	timeoutSeconds := defaultPositive(input.TimeoutSeconds, defaultDomainSearchTimeoutSeconds)
	recordsByID := domainSearchRecordsByID(input.Records)
	results := make([]DomainSearchRecordResult, 0, len(input.Pages))
	slog.DebugContext(ctx, "analyzing brreg domain search pages",
		"pages_count", len(input.Pages),
		"provider", llm.Provider,
		"model", llm.Model,
		"candidate_threshold", candidateThreshold,
		"max_candidates", maxCandidates,
		"timeout_seconds", timeoutSeconds,
	)
	for _, page := range input.Pages {
		record, ok := recordsByID[page.RawRecordID]
		if !ok {
			results = append(results, missingDomainSearchRecordResult(page))
			continue
		}
		result := DomainSearchRecordResult{
			RawRecordID:        page.RawRecordID,
			TaskAttemptID:      page.TaskAttemptID,
			OrganizationNumber: page.OrganizationNumber,
			Attempt:            page.Attempt,
			SearchEngine:       page.SearchEngine,
			SearchTerm:         page.SearchTerm,
			CandidateThreshold: candidateThreshold,
			MaxCandidates:      maxCandidates,
			TimeoutSeconds:     timeoutSeconds,
			Search:             page.Search,
			Provider:           llm.Provider,
			Model:              llm.Model,
		}
		if page.Status != brregdb.ResultStatusSucceeded.String() {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = page.Error
			results = append(results, result)
			continue
		}
		if page.Search == nil {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = &DomainSearchError{
				Message:       "Search page fetch did not return a crawl response.",
				Category:      "invalid_crawl_response",
				Code:          "missing_search_response",
				RetryStrategy: "retry_with_backoff",
			}
			results = append(results, result)
			continue
		}
		facts := domainFactsFromRecord(record)
		response, err := a.crawler.AnalyzeSearchPage(ctx, crawlclient.SearchAnalyzeRequest{
			CompanyName:        facts.CompanyName,
			OrganizationNumber: record.OrganizationNumber,
			Country:            facts.Country,
			AddressLines:       facts.AddressLines,
			City:               facts.City,
			PostalCode:         facts.PostalCode,
			BusinessActivity:   facts.BusinessActivity,
			StatutoryPurpose:   facts.StatutoryPurpose,
			IndustryCodes:      facts.IndustryCodes,
			SearchEngine:       page.SearchEngine,
			SearchTerm:         page.SearchTerm,
			Links:              page.Search.Links,
			Markdown:           page.Search.Markdown,
			CandidateThreshold: candidateThreshold,
			MaxCandidates:      maxCandidates,
			TimeoutSeconds:     timeoutSeconds,
			LLM:                llm,
		})
		result.AnalysisAttempted = true
		if err != nil {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = &DomainSearchError{
				Message:       "Search result analysis request failed.",
				Category:      "crawl_service",
				Code:          "search_analysis_request_failed",
				RetryStrategy: "retry_with_backoff",
				Detail:        map[string]any{"error": err.Error()},
			}
			results = append(results, result)
			continue
		}
		if response.Status != "succeeded" {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = domainSearchErrorFromServiceError(response.Error, "search_analysis_failed", "Search result analysis failed.")
			results = append(results, result)
			continue
		}
		result.Candidates = response.Candidates
		if len(response.Candidates) == 0 {
			result.Status = brregdb.ResultStatusNotFound.String()
		} else {
			result.Status = brregdb.ResultStatusPartial.String()
		}
		results = append(results, result)
	}
	return AnalyzeBrregDomainSearchPagesResult{Results: results}, nil
}

func (a *DomainSearchActions) domainSearchLLMSelection(ctx context.Context, provider string, model string) (crawlclient.LLMSelection, error) {
	selection := crawlclient.LLMSelection{Provider: defaultString(provider, "default"), Model: model}
	if selection.Provider == "default" {
		return selection, nil
	}
	if a.llmProviders == nil {
		return crawlclient.LLMSelection{}, errors.New("llm provider store not available")
	}
	config, err := a.llmProviders.RuntimeConfigBySlug(ctx, selection.Provider)
	if err != nil {
		return crawlclient.LLMSelection{}, errors.Wrap(err, "load llm provider runtime config")
	}
	selection.Provider = config.Slug
	selection.BaseURL = config.BaseURL
	selection.APIKey = config.APIKey
	if selection.Model == "" {
		selection.Model = config.Model
	}
	slog.DebugContext(ctx, "loaded brreg domain search llm provider runtime config",
		"provider", selection.Provider,
		"model", selection.Model,
		"has_inline_base_url", selection.BaseURL != "",
		"has_inline_api_key", selection.APIKey != "",
	)
	return selection, nil
}

type SubmitBrregDomainSearchResultsInput struct {
	WorkflowRunID string                     `json:"workflow_run_id"`
	Results       []DomainSearchRecordResult `json:"results"`
	MaxAttempts   int32                      `json:"max_attempts"`
}

type SubmitBrregDomainSearchResultsResult struct {
	RecordsSubmitted int32 `json:"records_submitted"`
	RecordsCompleted int32 `json:"records_completed"`
	RecordsFailed    int32 `json:"records_failed"`
}

func (a *DomainSearchActions) SubmitBrregDomainSearchResults(ctx context.Context, input SubmitBrregDomainSearchResultsInput) (SubmitBrregDomainSearchResultsResult, error) {
	if a == nil || a.gateway == nil {
		return SubmitBrregDomainSearchResultsResult{}, errors.New("brreg domain search gateway not available")
	}
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return SubmitBrregDomainSearchResultsResult{}, errors.Wrap(err, "parse brreg domain search workflow run id")
	}
	var summary SubmitBrregDomainSearchResultsResult
	for _, result := range input.Results {
		rawRecordID, err := uuid.Parse(result.RawRecordID)
		if err != nil {
			return SubmitBrregDomainSearchResultsResult{}, errors.Wrap(err, "parse brreg domain search raw record id")
		}
		taskAttemptID, err := uuid.Parse(result.TaskAttemptID)
		if err != nil {
			return SubmitBrregDomainSearchResultsResult{}, errors.Wrap(err, "parse brreg domain search task attempt id")
		}
		if err := a.recordDomainSearchActionArtifacts(ctx, workflowRunID, rawRecordID, taskAttemptID, result); err != nil {
			return SubmitBrregDomainSearchResultsResult{}, err
		}
		command, err := submitDomainSearchCommandFromResult(result, rawRecordID, taskAttemptID, input.MaxAttempts)
		if err != nil {
			return SubmitBrregDomainSearchResultsResult{}, err
		}
		if err := a.gateway.SubmitDomainResult(ctx, command); err != nil {
			return SubmitBrregDomainSearchResultsResult{}, errors.Wrap(err, "submit brreg domain search result")
		}
		summary.RecordsSubmitted++
		if brregdb.ResultStatus(result.Status) == brregdb.ResultStatusFailed {
			summary.RecordsFailed++
		} else {
			summary.RecordsCompleted++
		}
	}
	return summary, nil
}

func (a *DomainSearchActions) recordDomainSearchActionArtifacts(
	ctx context.Context,
	workflowRunID uuid.UUID,
	rawRecordID uuid.UUID,
	taskAttemptID uuid.UUID,
	result DomainSearchRecordResult,
) error {
	if result.Search != nil || (!result.AnalysisAttempted && result.Error != nil) {
		searchInput, err := marshalJSON(domainSearchActionInput{
			SearchEngine:   result.SearchEngine,
			SearchTerm:     result.SearchTerm,
			TimeoutSeconds: defaultPositive(result.TimeoutSeconds, defaultDomainSearchTimeoutSeconds),
		})
		if err != nil {
			return err
		}
		if result.Search != nil && result.Search.Status == "succeeded" {
			searchPayload, err := marshalJSON(result.Search)
			if err != nil {
				return err
			}
			if err := a.gateway.RecordDomainActionSuccess(ctx, brregdb.RecordDomainActionSuccessCommand{
				WorkflowRunID:   workflowRunID,
				TaskAttemptID:   taskAttemptID,
				RawRecordID:     rawRecordID,
				ActionType:      brregdb.DomainActionSearchPageFetch,
				Provider:        result.SearchEngine,
				Input:           searchInput,
				Attempt:         result.Attempt,
				ArtifactType:    brregdb.DomainArtifactSearchPage,
				ArtifactPayload: searchPayload,
				Metadata:        domainSearchActionMetadata("search_page_fetch", result),
			}); err != nil {
				return errors.Wrap(err, "record brreg domain search page artifact")
			}
		} else {
			failure := result.Error
			if failure == nil {
				failure = domainSearchErrorFromCrawlResponse(*result.Search, "domain_search_failed", "Search page fetch failed.")
			}
			if err := a.gateway.RecordDomainActionFailure(ctx, brregdb.RecordDomainActionFailureCommand{
				WorkflowRunID: workflowRunID,
				TaskAttemptID: taskAttemptID,
				RawRecordID:   rawRecordID,
				ActionType:    brregdb.DomainActionSearchPageFetch,
				Provider:      result.SearchEngine,
				Input:         searchInput,
				Attempt:       result.Attempt,
				Error:         failure.Message,
				ErrorCategory: failure.Category,
				ErrorCode:     failure.Code,
				RetryStrategy: failure.RetryStrategy,
				Metadata:      domainSearchActionMetadata("search_page_fetch", result),
			}); err != nil {
				return errors.Wrap(err, "record brreg domain search page failure")
			}
		}
	}
	if result.AnalysisAttempted {
		analysisInput, err := marshalJSON(domainSearchAnalysisActionInput{
			SearchEngine:       result.SearchEngine,
			SearchTerm:         result.SearchTerm,
			CandidateThreshold: defaultPositive(result.CandidateThreshold, defaultDomainSearchCandidateThreshold),
			MaxCandidates:      defaultPositive(result.MaxCandidates, defaultDomainSearchMaxCandidates),
		})
		if err != nil {
			return err
		}
		if result.Status != brregdb.ResultStatusFailed.String() {
			payload, err := marshalJSON(searchCandidatesArtifact{
				Status:     "succeeded",
				Candidates: result.Candidates,
			})
			if err != nil {
				return err
			}
			if err := a.gateway.RecordDomainActionSuccess(ctx, brregdb.RecordDomainActionSuccessCommand{
				WorkflowRunID:   workflowRunID,
				TaskAttemptID:   taskAttemptID,
				RawRecordID:     rawRecordID,
				ActionType:      brregdb.DomainActionSearchResultAnalysis,
				Provider:        result.Provider,
				Model:           result.Model,
				Input:           analysisInput,
				Attempt:         result.Attempt,
				ArtifactType:    brregdb.DomainArtifactSearchCandidates,
				ArtifactPayload: payload,
				Metadata:        domainSearchActionMetadata("search_result_analysis", result),
			}); err != nil {
				return errors.Wrap(err, "record brreg domain search candidate artifact")
			}
		} else {
			failure := result.Error
			if failure == nil {
				failure = &DomainSearchError{
					Message:       "Search result analysis failed.",
					Category:      "crawl_service",
					Code:          "search_analysis_failed",
					RetryStrategy: "retry_with_backoff",
				}
			}
			if err := a.gateway.RecordDomainActionFailure(ctx, brregdb.RecordDomainActionFailureCommand{
				WorkflowRunID: workflowRunID,
				TaskAttemptID: taskAttemptID,
				RawRecordID:   rawRecordID,
				ActionType:    brregdb.DomainActionSearchResultAnalysis,
				Provider:      result.Provider,
				Model:         result.Model,
				Input:         analysisInput,
				Attempt:       result.Attempt,
				Error:         failure.Message,
				ErrorCategory: failure.Category,
				ErrorCode:     failure.Code,
				RetryStrategy: failure.RetryStrategy,
				Metadata:      domainSearchActionMetadata("search_result_analysis", result),
			}); err != nil {
				return errors.Wrap(err, "record brreg domain search candidate failure")
			}
		}
	}
	return nil
}

type domainSearchActionInput struct {
	SearchEngine   string `json:"search_engine"`
	SearchTerm     string `json:"search_term"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type domainSearchAnalysisActionInput struct {
	SearchEngine       string `json:"search_engine"`
	SearchTerm         string `json:"search_term"`
	CandidateThreshold int    `json:"candidate_threshold"`
	MaxCandidates      int    `json:"max_candidates"`
}

type searchCandidatesArtifact struct {
	Status     string                   `json:"status"`
	Candidates []crawlclient.ScoredLink `json:"candidates"`
}

func submitDomainSearchCommandFromResult(
	result DomainSearchRecordResult,
	rawRecordID uuid.UUID,
	taskAttemptID uuid.UUID,
	maxAttempts int32,
) (brregdb.SubmitDomainResultCommand, error) {
	payload, err := marshalJSON(domainSearchPayloadFromResult(result))
	if err != nil {
		return brregdb.SubmitDomainResultCommand{}, err
	}
	metadata, err := marshalJSON(domainSearchResultMetadata{
		OrganizationNumber: result.OrganizationNumber,
		SearchEngine:       result.SearchEngine,
		SearchTerm:         result.SearchTerm,
		Provider:           result.Provider,
		Model:              result.Model,
		CandidatesCount:    len(result.Candidates),
	})
	if err != nil {
		return brregdb.SubmitDomainResultCommand{}, err
	}
	return brregdb.SubmitDomainResultCommand{
		Result: db.InsertBrregWorkflowDomainResultParams{
			RawRecordID:   rawRecordID,
			TaskAttemptID: taskAttemptID,
			Status:        result.Status,
			BestDomain:    nil,
			DomainPayload: payload,
			Error:         domainSearchErrorMessage(result.Error),
			Metadata:      metadata,
		},
		Failure:     taskFailureFromDomainSearchError(result.Status, result.Error),
		MaxAttempts: maxAttempts,
	}, nil
}

type domainSearchPayload struct {
	SchemaVersion string                        `json:"schema_version"`
	SearchEngine  string                        `json:"search_engine"`
	SearchTerm    string                        `json:"search_term"`
	Search        *crawlclient.Crawl4AiResponse `json:"search,omitempty"`
	Candidates    []crawlclient.ScoredLink      `json:"candidates"`
	Errors        []DomainSearchError           `json:"errors,omitempty"`
}

func domainSearchPayloadFromResult(result DomainSearchRecordResult) domainSearchPayload {
	payload := domainSearchPayload{
		SchemaVersion: "brreg.domain_search.v1",
		SearchEngine:  result.SearchEngine,
		SearchTerm:    result.SearchTerm,
		Search:        result.Search,
		Candidates:    result.Candidates,
	}
	if result.Error != nil {
		payload.Errors = []DomainSearchError{*result.Error}
	}
	return payload
}

type domainSearchResultMetadata struct {
	OrganizationNumber string `json:"organization_number,omitempty"`
	SearchEngine       string `json:"search_engine,omitempty"`
	SearchTerm         string `json:"search_term,omitempty"`
	Provider           string `json:"provider,omitempty"`
	Model              string `json:"model,omitempty"`
	CandidatesCount    int    `json:"candidates_count"`
}

func taskFailureFromDomainSearchError(status string, err *DomainSearchError) *brregdb.TaskFailure {
	if brregdb.ResultStatus(status) != brregdb.ResultStatusFailed || err == nil {
		return nil
	}
	return &brregdb.TaskFailure{
		ErrorCategory: err.Category,
		ErrorCode:     err.Code,
		RetryStrategy: err.RetryStrategy,
	}
}

func domainSearchErrorMessage(err *DomainSearchError) *string {
	if err == nil || err.Message == "" {
		return nil
	}
	return &err.Message
}

func domainSearchErrorFromCrawlResponse(response crawlclient.Crawl4AiResponse, code string, message string) *DomainSearchError {
	return domainSearchErrorFromMap(response.Error, code, message)
}

func domainSearchErrorFromServiceError(err *crawlclient.ServiceError, code string, message string) *DomainSearchError {
	if err == nil {
		return &DomainSearchError{
			Message:       message,
			Category:      "crawl_service",
			Code:          code,
			RetryStrategy: "retry_with_backoff",
		}
	}
	return &DomainSearchError{
		Message:       defaultString(err.Message, message),
		Category:      err.Category,
		Code:          defaultString(err.Code, code),
		RetryStrategy: defaultString(err.RetryStrategy, "retry_with_backoff"),
		Detail:        err.Detail,
	}
}

func domainSearchErrorFromMap(payload map[string]any, code string, message string) *DomainSearchError {
	if payload == nil {
		return &DomainSearchError{
			Message:       message,
			Category:      "crawl_service",
			Code:          code,
			RetryStrategy: "retry_with_backoff",
		}
	}
	return &DomainSearchError{
		Message:       defaultString(stringFromAny(payload["message"]), message),
		Category:      defaultString(stringFromAny(payload["category"]), "crawl_service"),
		Code:          defaultString(stringFromAny(payload["code"]), code),
		RetryStrategy: defaultString(stringFromAny(payload["retry_strategy"]), "retry_with_backoff"),
		Detail:        mapFromAny(payload["detail"]),
	}
}

func missingDomainSearchRecordResult(page DomainSearchPageResult) DomainSearchRecordResult {
	return DomainSearchRecordResult{
		RawRecordID:        page.RawRecordID,
		TaskAttemptID:      page.TaskAttemptID,
		OrganizationNumber: page.OrganizationNumber,
		Attempt:            page.Attempt,
		SearchEngine:       page.SearchEngine,
		SearchTerm:         page.SearchTerm,
		TimeoutSeconds:     page.TimeoutSeconds,
		Status:             brregdb.ResultStatusFailed.String(),
		Search:             page.Search,
		Error: &DomainSearchError{
			Message:       "Domain search analysis did not receive the claimed raw record.",
			Category:      "workflow_activity",
			Code:          "missing_claimed_record",
			RetryStrategy: "retry_with_backoff",
		},
	}
}

type brregDomainFacts struct {
	CompanyName      string
	Country          string
	AddressLines     []string
	City             string
	PostalCode       string
	BusinessActivity []string
	StatutoryPurpose []string
	IndustryCodes    []string
}

type brregRawDomainPayload struct {
	Name             string                  `json:"navn"`
	BusinessAddress  brregRawDomainAddress   `json:"forretningsadresse"`
	Activity         []string                `json:"aktivitet"`
	StatutoryPurpose []string                `json:"vedtektsfestetFormaal"`
	Industry1        brregRawDomainCodeValue `json:"naeringskode1"`
	Industry2        brregRawDomainCodeValue `json:"naeringskode2"`
	Industry3        brregRawDomainCodeValue `json:"naeringskode3"`
	SupportIndustry  brregRawDomainCodeValue `json:"hjelpeenhetskode"`
}

type brregRawDomainAddress struct {
	AddressLines []string `json:"adresse"`
	PostalCode   string   `json:"postnummer"`
	City         string   `json:"poststed"`
	CountryCode  string   `json:"landkode"`
}

type brregRawDomainCodeValue struct {
	Code        string `json:"kode"`
	Description string `json:"beskrivelse"`
}

func domainFactsFromRecord(record ClaimedDomainSearchRecord) brregDomainFacts {
	var raw brregRawDomainPayload
	_ = json.Unmarshal(record.RawPayload, &raw)
	country := defaultString(strings.TrimSpace(raw.BusinessAddress.CountryCode), defaultDomainSearchCountry)
	companyName := firstNonEmpty(record.OrganizationName, raw.Name, record.OrganizationNumber)
	return brregDomainFacts{
		CompanyName:      companyName,
		Country:          country,
		AddressLines:     cleanStringSlice(raw.BusinessAddress.AddressLines),
		City:             strings.TrimSpace(raw.BusinessAddress.City),
		PostalCode:       strings.TrimSpace(raw.BusinessAddress.PostalCode),
		BusinessActivity: cleanStringSlice(raw.Activity),
		StatutoryPurpose: cleanStringSlice(raw.StatutoryPurpose),
		IndustryCodes:    brregDomainIndustryCodes(raw),
	}
}

func domainSearchTerm(facts brregDomainFacts) string {
	return strings.Join([]string{facts.CompanyName, facts.Country, "website"}, " ")
}

func brregDomainIndustryCodes(raw brregRawDomainPayload) []string {
	codes := []brregRawDomainCodeValue{raw.Industry1, raw.Industry2, raw.Industry3, raw.SupportIndustry}
	values := make([]string, 0, len(codes))
	for _, code := range codes {
		label := strings.TrimSpace(strings.Join([]string{code.Code, code.Description}, " "))
		if label != "" {
			values = append(values, label)
		}
	}
	return values
}

func cleanStringSlice(values []string) []string {
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func domainSearchRecordsByID(records []ClaimedDomainSearchRecord) map[string]ClaimedDomainSearchRecord {
	byID := make(map[string]ClaimedDomainSearchRecord, len(records))
	for _, record := range records {
		byID[record.RawRecordID] = record
	}
	return byID
}

func domainSearchActionMetadata(stage string, result DomainSearchRecordResult) json.RawMessage {
	payload, err := marshalJSON(map[string]any{
		"stage":               stage,
		"organization_number": result.OrganizationNumber,
		"search_engine":       result.SearchEngine,
		"search_term":         result.SearchTerm,
		"provider":            result.Provider,
		"model":               result.Model,
		"status":              result.Status,
	})
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return payload
}

func marshalJSON(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "marshal brreg domain search json")
	}
	return json.RawMessage(data), nil
}

func defaultPositive(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func mapFromAny(value any) map[string]any {
	if value == nil {
		return nil
	}
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{"value": value}
}

func firstClaimedDomainSearchRawRecordID(rows []db.ClaimBrregWorkflowTaskSelectionBatchRow) string {
	if len(rows) == 0 {
		return ""
	}
	return rows[0].RawRecordID.String()
}
