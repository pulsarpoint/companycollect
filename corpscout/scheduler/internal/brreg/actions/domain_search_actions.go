package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

const (
	defaultDomainSearchLimit              = 1000
	defaultDomainSearchBatchSize          = 10
	defaultDomainSearchMaxAttempts        = 3
	defaultDomainSearchEngine             = "duckduckgo"
	defaultDomainSearchCountry            = "NO"
	defaultDomainSearchCandidateThreshold = 50
	defaultDomainSearchDomainThreshold    = 70
	defaultDomainSearchMaxCandidates      = 10
	defaultDomainSearchMaxSiteChecks      = 3
	defaultDomainSearchTimeoutSeconds     = 120
)

type DomainSearchActions struct {
	gateway      *brregdb.Gateway
	crawler      *crawlclient.Client
	llmProviders *llmproviders.Store
	s3           *s3client.Client
}

func NewDomainSearchActions(
	gateway *brregdb.Gateway,
	crawler *crawlclient.Client,
	llmProviders *llmproviders.Store,
	s3 *s3client.Client,
) *DomainSearchActions {
	return &DomainSearchActions{gateway: gateway, crawler: crawler, llmProviders: llmProviders, s3: s3}
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
	SiteChecks         []DomainCandidateSiteCheck    `json:"site_checks,omitempty"`
	Domains            []DomainDiscoveredDomain      `json:"domains,omitempty"`
	RelatedSites       []DomainRelatedSite           `json:"related_sites,omitempty"`
	BestDomain         string                        `json:"best_domain,omitempty"`
	AnalysisAttempted  bool                          `json:"analysis_attempted"`
	Provider           string                        `json:"provider,omitempty"`
	Model              string                        `json:"model,omitempty"`
	DomainThreshold    int                           `json:"domain_threshold,omitempty"`
	Error              *DomainSearchError            `json:"error,omitempty"`
}

type CrawlBrregDomainCandidateSitesInput struct {
	Results        []DomainSearchRecordResult `json:"results"`
	MaxSiteChecks  int                        `json:"max_site_checks,omitempty"`
	TimeoutSeconds int                        `json:"timeout_seconds,omitempty"`
}

type CrawlBrregDomainCandidateSitesResult struct {
	Results []DomainCandidateSiteCrawl `json:"results"`
}

type DomainCandidateSiteCrawl struct {
	RawRecordID        string                        `json:"raw_record_id"`
	TaskAttemptID      string                        `json:"task_attempt_id"`
	OrganizationNumber string                        `json:"organization_number"`
	Attempt            int32                         `json:"attempt"`
	Candidate          crawlclient.ScoredLink        `json:"candidate"`
	TimeoutSeconds     int                           `json:"timeout_seconds"`
	Status             string                        `json:"status"`
	Crawl              *crawlclient.Crawl4AiResponse `json:"crawl,omitempty"`
	MarkdownS3Key      string                        `json:"markdown_s3_key,omitempty"`
	Error              *DomainSearchError            `json:"error,omitempty"`
}

type AnalyzeBrregDomainCandidateSitesInput struct {
	Records         []ClaimedDomainSearchRecord `json:"records"`
	Results         []DomainSearchRecordResult  `json:"results"`
	SiteCrawls      []DomainCandidateSiteCrawl  `json:"site_crawls"`
	Provider        string                      `json:"provider,omitempty"`
	Model           string                      `json:"model,omitempty"`
	DomainThreshold int                         `json:"domain_threshold,omitempty"`
	TimeoutSeconds  int                         `json:"timeout_seconds,omitempty"`
}

type AnalyzeBrregDomainCandidateSitesResult struct {
	Results []DomainSearchRecordResult `json:"results"`
}

type DomainCandidateSiteCheck struct {
	Candidate     crawlclient.ScoredLink        `json:"candidate"`
	Crawl         *crawlclient.Crawl4AiResponse `json:"crawl,omitempty"`
	MarkdownS3Key string                        `json:"markdown_s3_key,omitempty"`
	Analysis      map[string]any                `json:"analysis,omitempty"`
	Status        string                        `json:"status"`
	Error         *DomainSearchError            `json:"error,omitempty"`
}

type DomainDiscoveredDomain struct {
	Domain           string         `json:"domain"`
	NormalizedDomain string         `json:"normalized_domain"`
	Score            int            `json:"score"`
	Decision         string         `json:"decision"`
	Source           string         `json:"source"`
	Evidence         map[string]any `json:"evidence,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

type DomainRelatedSite struct {
	URL              string         `json:"url"`
	Domain           string         `json:"domain"`
	NormalizedDomain string         `json:"normalized_domain"`
	Score            int            `json:"score"`
	Decision         string         `json:"decision"`
	SiteType         string         `json:"site_type"`
	Relationship     string         `json:"relationship"`
	OwnedDomain      bool           `json:"owned_domain"`
	Reason           string         `json:"reason,omitempty"`
	Evidence         []string       `json:"evidence,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
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

func (a *DomainSearchActions) CrawlBrregDomainCandidateSites(ctx context.Context, input CrawlBrregDomainCandidateSitesInput) (CrawlBrregDomainCandidateSitesResult, error) {
	if a == nil || a.crawler == nil {
		return CrawlBrregDomainCandidateSitesResult{}, errors.New("brreg crawl client not available")
	}
	maxSiteChecks := defaultPositive(input.MaxSiteChecks, defaultDomainSearchMaxSiteChecks)
	timeoutSeconds := defaultPositive(input.TimeoutSeconds, defaultDomainSearchTimeoutSeconds)
	results := make([]DomainCandidateSiteCrawl, 0)
	slog.DebugContext(ctx, "crawling brreg domain candidate sites",
		"records_count", len(input.Results),
		"max_site_checks", maxSiteChecks,
		"timeout_seconds", timeoutSeconds,
	)
	for _, recordResult := range input.Results {
		if recordResult.Status == brregdb.ResultStatusFailed.String() {
			continue
		}
		candidates := domainSearchCandidatesForSiteChecks(recordResult.Candidates, maxSiteChecks)
		for _, candidate := range candidates {
			result := DomainCandidateSiteCrawl{
				RawRecordID:        recordResult.RawRecordID,
				TaskAttemptID:      recordResult.TaskAttemptID,
				OrganizationNumber: recordResult.OrganizationNumber,
				Attempt:            recordResult.Attempt,
				Candidate:          candidate,
				TimeoutSeconds:     timeoutSeconds,
			}
			response, err := a.crawler.CrawlPage(ctx, crawlclient.CrawlPageRequest{
				URL:            candidate.URL,
				TimeoutSeconds: timeoutSeconds,
				Metadata: map[string]any{
					"candidate_score":  candidate.Score,
					"candidate_source": candidate.Source,
					"candidate_domain": candidate.NormalizedDomain,
				},
			})
			if err != nil {
				result.Status = brregdb.ResultStatusFailed.String()
				result.Error = &DomainSearchError{
					Message:       "Candidate site crawl request failed.",
					Category:      "crawl_service",
					Code:          "candidate_site_crawl_request_failed",
					RetryStrategy: "retry_with_backoff",
					Detail:        map[string]any{"error": err.Error(), "url": candidate.URL},
				}
				results = append(results, result)
				continue
			}
			if response.Status != "succeeded" {
				result.Status = brregdb.ResultStatusFailed.String()
				result.Crawl = &response
				result.Error = domainSearchErrorFromCrawlResponse(response, "candidate_site_crawl_failed", "Candidate site crawl failed.")
				results = append(results, result)
				continue
			}
			markdownS3Key, err := a.uploadCandidateSiteMarkdown(ctx, recordResult, candidate, &response)
			if err != nil {
				result.Status = brregdb.ResultStatusFailed.String()
				result.Crawl = scrubCrawlMarkdown(response)
				result.Error = &DomainSearchError{
					Message:       "Candidate site markdown upload failed.",
					Category:      "object_storage",
					Code:          "candidate_site_markdown_upload_failed",
					RetryStrategy: "retry_with_backoff",
					Detail:        map[string]any{"error": err.Error(), "url": candidate.URL},
				}
				results = append(results, result)
				continue
			}
			result.Status = brregdb.ResultStatusSucceeded.String()
			result.MarkdownS3Key = markdownS3Key
			result.Crawl = scrubCrawlMarkdown(response)
			results = append(results, result)
		}
	}
	return CrawlBrregDomainCandidateSitesResult{Results: results}, nil
}

func (a *DomainSearchActions) AnalyzeBrregDomainCandidateSites(ctx context.Context, input AnalyzeBrregDomainCandidateSitesInput) (AnalyzeBrregDomainCandidateSitesResult, error) {
	if a == nil || a.crawler == nil {
		return AnalyzeBrregDomainCandidateSitesResult{}, errors.New("brreg crawl client not available")
	}
	llm, err := a.domainSearchLLMSelection(ctx, input.Provider, input.Model)
	if err != nil {
		return AnalyzeBrregDomainCandidateSitesResult{}, err
	}
	domainThreshold := defaultPositive(input.DomainThreshold, defaultDomainSearchDomainThreshold)
	timeoutSeconds := defaultPositive(input.TimeoutSeconds, defaultDomainSearchTimeoutSeconds)
	recordsByID := domainSearchRecordsByID(input.Records)
	crawlsByRawRecordID := domainCandidateSiteCrawlsByRawRecordID(input.SiteCrawls)
	results := make([]DomainSearchRecordResult, 0, len(input.Results))
	slog.DebugContext(ctx, "analyzing brreg domain candidate sites",
		"records_count", len(input.Results),
		"site_crawls_count", len(input.SiteCrawls),
		"domain_threshold", domainThreshold,
		"provider", llm.Provider,
		"model", llm.Model,
	)
	for _, searchResult := range input.Results {
		result := searchResult
		result.Provider = llm.Provider
		result.Model = llm.Model
		result.DomainThreshold = domainThreshold
		if searchResult.Status == brregdb.ResultStatusFailed.String() {
			results = append(results, result)
			continue
		}
		record, ok := recordsByID[searchResult.RawRecordID]
		if !ok {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = &DomainSearchError{
				Message:       "Candidate site analysis did not receive the claimed raw record.",
				Category:      "workflow_activity",
				Code:          "missing_claimed_record",
				RetryStrategy: "retry_with_backoff",
			}
			results = append(results, result)
			continue
		}
		siteCrawls := crawlsByRawRecordID[searchResult.RawRecordID]
		if len(searchResult.Candidates) > 0 && len(siteCrawls) == 0 {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = &DomainSearchError{
				Message:       "Domain search produced candidates but no candidate sites were crawled.",
				Category:      "workflow_activity",
				Code:          "missing_candidate_site_crawls",
				RetryStrategy: "retry_with_backoff",
			}
			results = append(results, result)
			continue
		}
		facts := domainFactsFromRecord(record)
		siteChecks := make([]DomainCandidateSiteCheck, 0, len(siteCrawls))
		for _, siteCrawl := range siteCrawls {
			siteCheck := DomainCandidateSiteCheck{
				Candidate:     siteCrawl.Candidate,
				Crawl:         siteCrawl.Crawl,
				MarkdownS3Key: siteCrawl.MarkdownS3Key,
				Status:        siteCrawl.Status,
				Error:         siteCrawl.Error,
			}
			if siteCrawl.Status != brregdb.ResultStatusSucceeded.String() {
				siteChecks = append(siteChecks, siteCheck)
				continue
			}
			markdown, err := a.candidateSiteMarkdown(ctx, siteCrawl)
			if err != nil {
				siteCheck.Status = brregdb.ResultStatusFailed.String()
				siteCheck.Error = &DomainSearchError{
					Message:       "Candidate site markdown could not be loaded for analysis.",
					Category:      "object_storage",
					Code:          "candidate_site_markdown_load_failed",
					RetryStrategy: "retry_with_backoff",
					Detail:        map[string]any{"error": err.Error(), "s3_key": siteCrawl.MarkdownS3Key},
				}
				siteChecks = append(siteChecks, siteCheck)
				continue
			}
			crawl := siteCrawl.Crawl
			if crawl == nil {
				siteCheck.Status = brregdb.ResultStatusFailed.String()
				siteCheck.Error = &DomainSearchError{
					Message:       "Candidate site crawl response is missing.",
					Category:      "invalid_crawl_response",
					Code:          "missing_candidate_site_crawl",
					RetryStrategy: "retry_with_backoff",
				}
				siteChecks = append(siteChecks, siteCheck)
				continue
			}
			response, err := a.crawler.AnalyzeCompanyPage(ctx, crawlclient.PageAnalyzeRequest{
				CompanyName:        facts.CompanyName,
				OrganizationNumber: record.OrganizationNumber,
				Country:            facts.Country,
				AddressLines:       facts.AddressLines,
				City:               facts.City,
				PostalCode:         facts.PostalCode,
				BusinessActivity:   facts.BusinessActivity,
				StatutoryPurpose:   facts.StatutoryPurpose,
				IndustryCodes:      facts.IndustryCodes,
				URL:                siteCrawl.Candidate.URL,
				FinalURL:           firstNonEmpty(crawl.FinalURL, siteCrawl.Candidate.URL),
				NormalizedDomain:   firstNonEmpty(siteCrawl.Candidate.NormalizedDomain, domainFromURL(firstNonEmpty(crawl.FinalURL, siteCrawl.Candidate.URL))),
				Markdown:           markdown,
				CandidateScore:     siteCrawl.Candidate.Score,
				CandidateReason:    siteCrawl.Candidate.Reason,
				TimeoutSeconds:     timeoutSeconds,
				LLM:                llm,
			})
			if err != nil {
				siteCheck.Status = brregdb.ResultStatusFailed.String()
				siteCheck.Error = &DomainSearchError{
					Message:       "Candidate site analysis request failed.",
					Category:      "crawl_service",
					Code:          "candidate_site_analysis_request_failed",
					RetryStrategy: "retry_with_backoff",
					Detail:        map[string]any{"error": err.Error(), "url": siteCrawl.Candidate.URL},
				}
				siteChecks = append(siteChecks, siteCheck)
				continue
			}
			if response.Status != "succeeded" {
				siteCheck.Status = brregdb.ResultStatusFailed.String()
				siteCheck.Error = domainSearchErrorFromServiceError(response.Error, "candidate_site_analysis_failed", "Candidate site analysis failed.")
				siteChecks = append(siteChecks, siteCheck)
				continue
			}
			siteCheck.Status = brregdb.ResultStatusSucceeded.String()
			siteCheck.Analysis = response.Analysis
			siteChecks = append(siteChecks, siteCheck)
		}
		result.SiteChecks = siteChecks
		result.Domains = acceptedDomainsFromSiteChecks(siteChecks, domainThreshold)
		result.RelatedSites = relatedSitesFromSiteChecks(siteChecks, domainThreshold)
		if len(result.Domains) > 0 {
			result.Status = brregdb.ResultStatusSucceeded.String()
			result.BestDomain = result.Domains[0].NormalizedDomain
			result.Error = nil
		} else if allSiteChecksFailed(siteChecks) {
			result.Status = brregdb.ResultStatusFailed.String()
			result.Error = firstSiteCheckError(siteChecks)
		} else {
			result.Status = brregdb.ResultStatusNotFound.String()
			result.Error = nil
		}
		results = append(results, result)
	}
	return AnalyzeBrregDomainCandidateSitesResult{Results: results}, nil
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
		if domainSearchAnalysisSucceeded(result) {
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
	for _, siteCheck := range result.SiteChecks {
		if err := a.recordCandidateSiteCrawlArtifact(ctx, workflowRunID, rawRecordID, taskAttemptID, result, siteCheck); err != nil {
			return err
		}
		if siteCheck.Analysis != nil || siteCheck.Error != nil {
			if err := a.recordCandidateSiteAnalysisArtifact(ctx, workflowRunID, rawRecordID, taskAttemptID, result, siteCheck); err != nil {
				return err
			}
		}
	}
	if len(result.SiteChecks) > 0 || len(result.Domains) > 0 || len(result.RelatedSites) > 0 {
		decisionInput, err := marshalJSON(domainDecisionActionInput{
			CandidateThreshold: defaultPositive(result.CandidateThreshold, defaultDomainSearchCandidateThreshold),
			DomainThreshold:    defaultPositive(result.DomainThreshold, defaultDomainSearchDomainThreshold),
		})
		if err != nil {
			return err
		}
		decisionPayload, err := marshalJSON(domainDecisionArtifact{
			Status:       result.Status,
			BestDomain:   result.BestDomain,
			Domains:      result.Domains,
			RelatedSites: result.RelatedSites,
		})
		if err != nil {
			return err
		}
		if err := a.gateway.RecordDomainActionSuccess(ctx, brregdb.RecordDomainActionSuccessCommand{
			WorkflowRunID:   workflowRunID,
			TaskAttemptID:   taskAttemptID,
			RawRecordID:     rawRecordID,
			ActionType:      brregdb.DomainActionDecision,
			Provider:        result.Provider,
			Model:           result.Model,
			Input:           decisionInput,
			Attempt:         result.Attempt,
			ArtifactType:    brregdb.DomainArtifactDecision,
			ArtifactPayload: decisionPayload,
			Metadata:        domainSearchActionMetadata("domain_decision", result),
		}); err != nil {
			return errors.Wrap(err, "record brreg domain decision artifact")
		}
	}
	return nil
}

func (a *DomainSearchActions) recordCandidateSiteCrawlArtifact(
	ctx context.Context,
	workflowRunID uuid.UUID,
	rawRecordID uuid.UUID,
	taskAttemptID uuid.UUID,
	result DomainSearchRecordResult,
	siteCheck DomainCandidateSiteCheck,
) error {
	input, err := marshalJSON(candidateSiteCrawlActionInput{
		URL:            siteCheck.Candidate.URL,
		TimeoutSeconds: defaultPositive(result.TimeoutSeconds, defaultDomainSearchTimeoutSeconds),
	})
	if err != nil {
		return err
	}
	if candidateSiteCrawlSucceeded(siteCheck) {
		payload, err := marshalJSON(candidateSiteCrawlArtifact{
			Status:        brregdb.ResultStatusSucceeded.String(),
			Candidate:     siteCheck.Candidate,
			Crawl:         siteCheck.Crawl,
			MarkdownS3Key: siteCheck.MarkdownS3Key,
		})
		if err != nil {
			return err
		}
		if err := a.gateway.RecordDomainActionSuccess(ctx, brregdb.RecordDomainActionSuccessCommand{
			WorkflowRunID:   workflowRunID,
			TaskAttemptID:   taskAttemptID,
			RawRecordID:     rawRecordID,
			ActionType:      brregdb.DomainActionCandidateSiteCrawl,
			Provider:        result.SearchEngine,
			Input:           input,
			Attempt:         result.Attempt,
			ArtifactType:    brregdb.DomainArtifactCrawlPage,
			ArtifactPayload: payload,
			Metadata:        domainSearchActionMetadata("candidate_site_crawl", result),
		}); err != nil {
			return errors.Wrap(err, "record brreg candidate site crawl artifact")
		}
		return nil
	}
	failure := siteCheck.Error
	if failure == nil {
		failure = &DomainSearchError{
			Message:       "Candidate site crawl failed.",
			Category:      "crawl_service",
			Code:          "candidate_site_crawl_failed",
			RetryStrategy: "retry_with_backoff",
		}
	}
	if err := a.gateway.RecordDomainActionFailure(ctx, brregdb.RecordDomainActionFailureCommand{
		WorkflowRunID: workflowRunID,
		TaskAttemptID: taskAttemptID,
		RawRecordID:   rawRecordID,
		ActionType:    brregdb.DomainActionCandidateSiteCrawl,
		Provider:      result.SearchEngine,
		Input:         input,
		Attempt:       result.Attempt,
		Error:         failure.Message,
		ErrorCategory: failure.Category,
		ErrorCode:     failure.Code,
		RetryStrategy: failure.RetryStrategy,
		Metadata:      domainSearchActionMetadata("candidate_site_crawl", result),
	}); err != nil {
		return errors.Wrap(err, "record brreg candidate site crawl failure")
	}
	return nil
}

func domainSearchAnalysisSucceeded(result DomainSearchRecordResult) bool {
	if !result.AnalysisAttempted {
		return false
	}
	if len(result.Candidates) > 0 || result.Error == nil {
		return true
	}
	return !strings.HasPrefix(result.Error.Code, "search_analysis")
}

func candidateSiteCrawlSucceeded(siteCheck DomainCandidateSiteCheck) bool {
	return siteCheck.Crawl != nil && siteCheck.Crawl.Status == "succeeded"
}

func (a *DomainSearchActions) recordCandidateSiteAnalysisArtifact(
	ctx context.Context,
	workflowRunID uuid.UUID,
	rawRecordID uuid.UUID,
	taskAttemptID uuid.UUID,
	result DomainSearchRecordResult,
	siteCheck DomainCandidateSiteCheck,
) error {
	input, err := marshalJSON(candidateSiteAnalysisActionInput{
		URL:              siteCheck.Candidate.URL,
		FinalURL:         crawlFinalURL(siteCheck),
		NormalizedDomain: firstNonEmpty(siteCheck.Candidate.NormalizedDomain, domainFromURL(crawlFinalURL(siteCheck))),
		CandidateScore:   siteCheck.Candidate.Score,
		DomainThreshold:  defaultPositive(result.DomainThreshold, defaultDomainSearchDomainThreshold),
	})
	if err != nil {
		return err
	}
	if siteCheck.Analysis != nil && siteCheck.Status == brregdb.ResultStatusSucceeded.String() {
		payload, err := marshalJSON(candidateSiteAnalysisArtifact{
			Status:        siteCheck.Status,
			Candidate:     siteCheck.Candidate,
			MarkdownS3Key: siteCheck.MarkdownS3Key,
			Analysis:      siteCheck.Analysis,
		})
		if err != nil {
			return err
		}
		if err := a.gateway.RecordDomainActionSuccess(ctx, brregdb.RecordDomainActionSuccessCommand{
			WorkflowRunID:   workflowRunID,
			TaskAttemptID:   taskAttemptID,
			RawRecordID:     rawRecordID,
			ActionType:      brregdb.DomainActionCandidateSiteAnalysis,
			Provider:        result.Provider,
			Model:           result.Model,
			Input:           input,
			Attempt:         result.Attempt,
			ArtifactType:    brregdb.DomainArtifactSiteAnalysis,
			ArtifactPayload: payload,
			Metadata:        domainSearchActionMetadata("candidate_site_analysis", result),
		}); err != nil {
			return errors.Wrap(err, "record brreg candidate site analysis artifact")
		}
		return nil
	}
	failure := siteCheck.Error
	if failure == nil {
		failure = &DomainSearchError{
			Message:       "Candidate site analysis failed.",
			Category:      "crawl_service",
			Code:          "candidate_site_analysis_failed",
			RetryStrategy: "retry_with_backoff",
		}
	}
	if err := a.gateway.RecordDomainActionFailure(ctx, brregdb.RecordDomainActionFailureCommand{
		WorkflowRunID: workflowRunID,
		TaskAttemptID: taskAttemptID,
		RawRecordID:   rawRecordID,
		ActionType:    brregdb.DomainActionCandidateSiteAnalysis,
		Provider:      result.Provider,
		Model:         result.Model,
		Input:         input,
		Attempt:       result.Attempt,
		Error:         failure.Message,
		ErrorCategory: failure.Category,
		ErrorCode:     failure.Code,
		RetryStrategy: failure.RetryStrategy,
		Metadata:      domainSearchActionMetadata("candidate_site_analysis", result),
	}); err != nil {
		return errors.Wrap(err, "record brreg candidate site analysis failure")
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

type candidateSiteCrawlActionInput struct {
	URL            string `json:"url"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type candidateSiteAnalysisActionInput struct {
	URL              string `json:"url"`
	FinalURL         string `json:"final_url"`
	NormalizedDomain string `json:"normalized_domain"`
	CandidateScore   int    `json:"candidate_score"`
	DomainThreshold  int    `json:"domain_threshold"`
}

type candidateSiteCrawlArtifact struct {
	Status        string                        `json:"status"`
	Candidate     crawlclient.ScoredLink        `json:"candidate"`
	Crawl         *crawlclient.Crawl4AiResponse `json:"crawl,omitempty"`
	MarkdownS3Key string                        `json:"markdown_s3_key,omitempty"`
}

type candidateSiteAnalysisArtifact struct {
	Status        string                 `json:"status"`
	Candidate     crawlclient.ScoredLink `json:"candidate"`
	MarkdownS3Key string                 `json:"markdown_s3_key,omitempty"`
	Analysis      map[string]any         `json:"analysis"`
}

type domainDecisionActionInput struct {
	CandidateThreshold int `json:"candidate_threshold"`
	DomainThreshold    int `json:"domain_threshold"`
}

type domainDecisionArtifact struct {
	Status       string                   `json:"status"`
	BestDomain   string                   `json:"best_domain,omitempty"`
	Domains      []DomainDiscoveredDomain `json:"domains,omitempty"`
	RelatedSites []DomainRelatedSite      `json:"related_sites,omitempty"`
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
		SiteChecksCount:    len(result.SiteChecks),
		DomainsCount:       len(result.Domains),
	})
	if err != nil {
		return brregdb.SubmitDomainResultCommand{}, err
	}
	var bestDomain *string
	if result.BestDomain != "" {
		bestDomain = &result.BestDomain
	}
	return brregdb.SubmitDomainResultCommand{
		Result: db.InsertBrregWorkflowDomainResultParams{
			RawRecordID:   rawRecordID,
			TaskAttemptID: taskAttemptID,
			Status:        result.Status,
			BestDomain:    bestDomain,
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
	SiteChecks    []DomainCandidateSiteCheck    `json:"site_checks,omitempty"`
	Domains       []DomainDiscoveredDomain      `json:"domains,omitempty"`
	RelatedSites  []DomainRelatedSite           `json:"related_sites,omitempty"`
	BestDomain    string                        `json:"best_domain,omitempty"`
	Thresholds    domainSearchThresholds        `json:"thresholds"`
	Errors        []DomainSearchError           `json:"errors,omitempty"`
}

func domainSearchPayloadFromResult(result DomainSearchRecordResult) domainSearchPayload {
	payload := domainSearchPayload{
		SchemaVersion: "brreg.domain_search.v1",
		SearchEngine:  result.SearchEngine,
		SearchTerm:    result.SearchTerm,
		Search:        result.Search,
		Candidates:    result.Candidates,
		SiteChecks:    result.SiteChecks,
		Domains:       result.Domains,
		RelatedSites:  result.RelatedSites,
		BestDomain:    result.BestDomain,
		Thresholds: domainSearchThresholds{
			Candidate: defaultPositive(result.CandidateThreshold, defaultDomainSearchCandidateThreshold),
			Domain:    defaultPositive(result.DomainThreshold, defaultDomainSearchDomainThreshold),
		},
	}
	if result.Error != nil {
		payload.Errors = []DomainSearchError{*result.Error}
	}
	return payload
}

type domainSearchThresholds struct {
	Candidate int `json:"candidate"`
	Domain    int `json:"domain"`
}

type domainSearchResultMetadata struct {
	OrganizationNumber string `json:"organization_number,omitempty"`
	SearchEngine       string `json:"search_engine,omitempty"`
	SearchTerm         string `json:"search_term,omitempty"`
	Provider           string `json:"provider,omitempty"`
	Model              string `json:"model,omitempty"`
	CandidatesCount    int    `json:"candidates_count"`
	SiteChecksCount    int    `json:"site_checks_count"`
	DomainsCount       int    `json:"domains_count"`
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

func domainCandidateSiteCrawlsByRawRecordID(crawls []DomainCandidateSiteCrawl) map[string][]DomainCandidateSiteCrawl {
	byID := make(map[string][]DomainCandidateSiteCrawl)
	for _, crawl := range crawls {
		byID[crawl.RawRecordID] = append(byID[crawl.RawRecordID], crawl)
	}
	return byID
}

func domainSearchCandidatesForSiteChecks(candidates []crawlclient.ScoredLink, maxSiteChecks int) []crawlclient.ScoredLink {
	if maxSiteChecks <= 0 {
		maxSiteChecks = defaultDomainSearchMaxSiteChecks
	}
	selected := make([]crawlclient.ScoredLink, 0, min(len(candidates), maxSiteChecks))
	seen := make(map[string]struct{})
	for _, candidate := range candidates {
		key := firstNonEmpty(candidate.NormalizedDomain, domainFromURL(candidate.URL), candidate.URL)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		selected = append(selected, candidate)
		if len(selected) >= maxSiteChecks {
			break
		}
	}
	return selected
}

func (a *DomainSearchActions) uploadCandidateSiteMarkdown(
	ctx context.Context,
	result DomainSearchRecordResult,
	candidate crawlclient.ScoredLink,
	response *crawlclient.Crawl4AiResponse,
) (string, error) {
	if response == nil || strings.TrimSpace(response.Markdown) == "" {
		return "", nil
	}
	if a == nil || a.s3 == nil {
		return "", errors.New("s3 client not available for candidate site markdown")
	}
	markdownHash := firstNonEmpty(response.MarkdownHash, sha256Hex([]byte(response.Markdown)))
	response.MarkdownHash = markdownHash
	key := candidateSiteMarkdownS3Key(result, candidate, markdownHash)
	if err := a.s3.Upload(ctx, key, []byte(response.Markdown), "text/markdown; charset=utf-8"); err != nil {
		return "", errors.Wrap(err, "upload candidate site markdown")
	}
	return key, nil
}

func (a *DomainSearchActions) candidateSiteMarkdown(ctx context.Context, siteCrawl DomainCandidateSiteCrawl) (string, error) {
	if siteCrawl.MarkdownS3Key == "" {
		if siteCrawl.Crawl != nil {
			return siteCrawl.Crawl.Markdown, nil
		}
		return "", nil
	}
	if a == nil || a.s3 == nil {
		return "", errors.New("s3 client not available for candidate site markdown")
	}
	data, _, err := a.s3.Download(ctx, siteCrawl.MarkdownS3Key)
	if err != nil {
		return "", errors.Wrap(err, "download candidate site markdown")
	}
	return string(data), nil
}

func candidateSiteMarkdownS3Key(result DomainSearchRecordResult, candidate crawlclient.ScoredLink, markdownHash string) string {
	domain := sanitizeS3PathPart(firstNonEmpty(candidate.NormalizedDomain, domainFromURL(candidate.URL), "unknown-domain"))
	if markdownHash == "" {
		markdownHash = "unknown-hash"
	}
	return fmt.Sprintf(
		"brreg/domain-discovery/%s/%s/site-crawls/%s-%s.md",
		sanitizeS3PathPart(result.RawRecordID),
		sanitizeS3PathPart(result.TaskAttemptID),
		domain,
		sanitizeS3PathPart(markdownHash),
	)
}

func scrubCrawlMarkdown(response crawlclient.Crawl4AiResponse) *crawlclient.Crawl4AiResponse {
	response.Markdown = ""
	return &response
}

func acceptedDomainsFromSiteChecks(siteChecks []DomainCandidateSiteCheck, threshold int) []DomainDiscoveredDomain {
	rows := make([]DomainDiscoveredDomain, 0)
	seen := make(map[string]struct{})
	for _, siteCheck := range siteChecks {
		if !siteCheckAccepted(siteCheck, threshold) {
			continue
		}
		domain := siteCheckDomain(siteCheck)
		normalized := normalizeDomain(domain)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		rows = append(rows, DomainDiscoveredDomain{
			Domain:           domain,
			NormalizedDomain: normalized,
			Score:            scoreFromAnalysis(siteCheck.Analysis),
			Decision:         decisionFromAnalysis(siteCheck.Analysis),
			Source:           "domain_site_llm",
			Evidence: map[string]any{
				"reason":   stringFromAny(siteCheck.Analysis["reason"]),
				"evidence": stringSliceFromAny(siteCheck.Analysis["evidence"]),
			},
			Metadata: map[string]any{
				"url":             siteCheck.Candidate.URL,
				"markdown_hash":   crawlMarkdownHash(siteCheck.Crawl),
				"markdown_s3_key": siteCheck.MarkdownS3Key,
			},
		})
	}
	return rows
}

func relatedSitesFromSiteChecks(siteChecks []DomainCandidateSiteCheck, threshold int) []DomainRelatedSite {
	rows := make([]DomainRelatedSite, 0)
	seen := make(map[string]struct{})
	for _, siteCheck := range siteChecks {
		if siteCheck.Status != brregdb.ResultStatusSucceeded.String() || siteCheck.Analysis == nil {
			continue
		}
		if scoreFromAnalysis(siteCheck.Analysis) < threshold || decisionFromAnalysis(siteCheck.Analysis) == "rejected" {
			continue
		}
		relationship := relationshipFromAnalysis(siteCheck.Analysis)
		if relationship == "unrelated" {
			continue
		}
		domain := siteCheckDomain(siteCheck)
		normalized := normalizeDomain(domain)
		key := siteCheck.Candidate.URL + "|" + normalized
		if key == "|" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		rows = append(rows, DomainRelatedSite{
			URL:              siteCheck.Candidate.URL,
			Domain:           domain,
			NormalizedDomain: normalized,
			Score:            scoreFromAnalysis(siteCheck.Analysis),
			Decision:         decisionFromAnalysis(siteCheck.Analysis),
			SiteType:         siteTypeFromAnalysis(siteCheck.Analysis),
			Relationship:     relationship,
			OwnedDomain:      boolFromAny(siteCheck.Analysis["owned_domain"]),
			Reason:           stringFromAny(siteCheck.Analysis["reason"]),
			Evidence:         stringSliceFromAny(siteCheck.Analysis["evidence"]),
			Metadata: map[string]any{
				"markdown_hash":   crawlMarkdownHash(siteCheck.Crawl),
				"markdown_s3_key": siteCheck.MarkdownS3Key,
			},
		})
	}
	return rows
}

func siteCheckAccepted(siteCheck DomainCandidateSiteCheck, threshold int) bool {
	if siteCheck.Status != brregdb.ResultStatusSucceeded.String() || siteCheck.Analysis == nil {
		return false
	}
	if scoreFromAnalysis(siteCheck.Analysis) < threshold {
		return false
	}
	if decisionFromAnalysis(siteCheck.Analysis) == "rejected" {
		return false
	}
	return siteTypeFromAnalysis(siteCheck.Analysis) == "company_website" && boolFromAny(siteCheck.Analysis["owned_domain"])
}

func allSiteChecksFailed(siteChecks []DomainCandidateSiteCheck) bool {
	if len(siteChecks) == 0 {
		return false
	}
	for _, siteCheck := range siteChecks {
		if siteCheck.Status != brregdb.ResultStatusFailed.String() {
			return false
		}
	}
	return true
}

func firstSiteCheckError(siteChecks []DomainCandidateSiteCheck) *DomainSearchError {
	for _, siteCheck := range siteChecks {
		if siteCheck.Error != nil {
			return siteCheck.Error
		}
	}
	return &DomainSearchError{
		Message:       "All candidate site checks failed.",
		Category:      "crawl_service",
		Code:          "candidate_site_checks_failed",
		RetryStrategy: "retry_with_backoff",
	}
}

func crawlFinalURL(siteCheck DomainCandidateSiteCheck) string {
	if siteCheck.Crawl != nil {
		return firstNonEmpty(siteCheck.Crawl.FinalURL, siteCheck.Crawl.URL)
	}
	return siteCheck.Candidate.URL
}

func siteCheckDomain(siteCheck DomainCandidateSiteCheck) string {
	if siteCheck.Crawl != nil {
		if domain := domainFromURL(siteCheck.Crawl.FinalURL); domain != "" {
			return domain
		}
	}
	return firstNonEmpty(siteCheck.Candidate.NormalizedDomain, siteCheck.Candidate.Domain, domainFromURL(siteCheck.Candidate.URL))
}

func crawlMarkdownHash(crawl *crawlclient.Crawl4AiResponse) string {
	if crawl == nil {
		return ""
	}
	return crawl.MarkdownHash
}

func scoreFromAnalysis(analysis map[string]any) int {
	value := analysis["score"]
	if value == nil {
		value = analysis["confidence"]
	}
	switch typed := value.(type) {
	case int:
		return clampScore(typed)
	case int32:
		return clampScore(int(typed))
	case int64:
		return clampScore(int(typed))
	case float64:
		return clampScore(int(typed))
	case float32:
		return clampScore(int(typed))
	case json.Number:
		parsed, _ := typed.Int64()
		return clampScore(int(parsed))
	case string:
		var parsed int
		if _, err := fmt.Sscanf(typed, "%d", &parsed); err == nil {
			return clampScore(parsed)
		}
	}
	return 0
}

func clampScore(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func decisionFromAnalysis(analysis map[string]any) string {
	value := strings.ToLower(strings.TrimSpace(stringFromAny(analysis["decision"])))
	switch value {
	case "accepted", "accept", "related", "matched", "match", "likely_related":
		return "accepted"
	case "rejected", "reject", "unrelated", "no_match":
		return "rejected"
	default:
		return "uncertain"
	}
}

func siteTypeFromAnalysis(analysis map[string]any) string {
	value := strings.ToLower(strings.TrimSpace(stringFromAny(analysis["site_type"])))
	switch value {
	case "company_website", "social_profile", "directory_profile", "registry_profile", "reference_page", "marketplace_profile", "unrelated":
		return value
	default:
		return "unrelated"
	}
}

func relationshipFromAnalysis(analysis map[string]any) string {
	value := strings.ToLower(strings.TrimSpace(stringFromAny(analysis["relationship"])))
	switch value {
	case "primary_web_presence", "evidence_profile", "supporting_reference", "unrelated":
		return value
	default:
		return "unrelated"
	}
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

func boolFromAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.ToLower(strings.TrimSpace(typed))
		return normalized == "true" || normalized == "yes" || normalized == "1"
	default:
		return false
	}
}

func stringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return cleanStringSlice(typed)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if value := strings.TrimSpace(stringFromAny(item)); value != "" {
				values = append(values, value)
			}
		}
		return values
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{strings.TrimSpace(typed)}
	default:
		return nil
	}
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func sanitizeS3PathPart(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "unknown"
	}
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	cleaned := strings.Trim(builder.String(), "-.")
	if cleaned == "" {
		return "unknown"
	}
	return cleaned
}

func domainFromURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	return normalizeDomain(parsed.Host)
}

func normalizeDomain(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	if strings.Contains(value, "@") {
		parts := strings.Split(value, "@")
		value = parts[len(parts)-1]
	}
	if host, _, ok := strings.Cut(value, ":"); ok {
		value = host
	}
	value = strings.Trim(value, ".")
	value = strings.TrimPrefix(value, "www.")
	return value
}

func firstClaimedDomainSearchRawRecordID(rows []db.ClaimBrregWorkflowTaskSelectionBatchRow) string {
	if len(rows) == 0 {
		return ""
	}
	return rows[0].RawRecordID.String()
}
