package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	SearchBrregDomainsTaskQueue    = "brreg-domain-search"
	SearchBrregDomainsWorkflowName = "SearchBrregDomains"

	prepareBrregDomainSearchWorkflowActivity    = "PrepareBrregDomainSearchWorkflow"
	claimBrregDomainSearchBatchActivity         = "ClaimBrregDomainSearchBatch"
	fetchBrregDomainSearchPagesActivity         = "FetchBrregDomainSearchPages"
	analyzeBrregDomainSearchPagesActivity       = "AnalyzeBrregDomainSearchPages"
	crawlBrregDomainCandidateSitesActivity      = "CrawlBrregDomainCandidateSites"
	analyzeBrregDomainCandidateSitesActivity    = "AnalyzeBrregDomainCandidateSites"
	submitBrregDomainSearchResultsActivity      = "SubmitBrregDomainSearchResults"
	failRunningBrregDomainSearchTasksActivity   = "FailRunningBrregDomainSearchTasksForWorkflow"
	finishBrregDomainSearchWorkflowActivity     = "FinishBrregDomainSearchWorkflow"
	defaultDomainSearchLimit                    = 1000
	defaultDomainSearchBatchSize                = 10
	defaultDomainSearchMaxAttempts              = 3
	defaultDomainSearchLeaseSeconds             = 900
	defaultDomainSearchMaxParallelTasks         = 5
	defaultDomainSearchCandidateThreshold       = 50
	defaultDomainSearchDomainThreshold          = 70
	defaultDomainSearchMaxCandidates            = 10
	defaultDomainSearchMaxSiteChecks            = 3
	defaultDomainSearchTimeoutSeconds           = 120
	defaultDomainSearchMutationActivityAttempts = 1
	defaultDomainSearchServiceActivityAttempts  = 3
)

type PrepareBrregDomainSearchWorkflowInput = actions.PrepareBrregDomainSearchWorkflowInput
type PrepareBrregDomainSearchWorkflowResult = actions.PrepareBrregDomainSearchWorkflowResult
type FinishBrregDomainSearchWorkflowInput = actions.FinishBrregDomainSearchWorkflowInput
type FinishBrregDomainSearchWorkflowResult = actions.FinishBrregDomainSearchWorkflowResult
type FailRunningBrregDomainSearchTasksForWorkflowInput = actions.FailRunningBrregDomainSearchTasksForWorkflowInput
type FailRunningBrregDomainSearchTasksForWorkflowResult = actions.FailRunningBrregDomainSearchTasksForWorkflowResult
type ClaimBrregDomainSearchBatchInput = actions.ClaimBrregDomainSearchBatchInput
type ClaimBrregDomainSearchBatchResult = actions.ClaimBrregDomainSearchBatchResult
type ClaimedDomainSearchRecord = actions.ClaimedDomainSearchRecord
type FetchBrregDomainSearchPagesInput = actions.FetchBrregDomainSearchPagesInput
type FetchBrregDomainSearchPagesResult = actions.FetchBrregDomainSearchPagesResult
type DomainSearchPageResult = actions.DomainSearchPageResult
type AnalyzeBrregDomainSearchPagesInput = actions.AnalyzeBrregDomainSearchPagesInput
type AnalyzeBrregDomainSearchPagesResult = actions.AnalyzeBrregDomainSearchPagesResult
type DomainSearchRecordResult = actions.DomainSearchRecordResult
type CrawlBrregDomainCandidateSitesInput = actions.CrawlBrregDomainCandidateSitesInput
type CrawlBrregDomainCandidateSitesResult = actions.CrawlBrregDomainCandidateSitesResult
type DomainCandidateSiteCrawl = actions.DomainCandidateSiteCrawl
type AnalyzeBrregDomainCandidateSitesInput = actions.AnalyzeBrregDomainCandidateSitesInput
type AnalyzeBrregDomainCandidateSitesResult = actions.AnalyzeBrregDomainCandidateSitesResult
type SubmitBrregDomainSearchResultsInput = actions.SubmitBrregDomainSearchResultsInput
type SubmitBrregDomainSearchResultsResult = actions.SubmitBrregDomainSearchResultsResult

type SearchBrregDomainsInput struct {
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	BatchSize   int               `json:"batch_size,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`

	MaxParallelTasks   int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds       int    `json:"lease_seconds,omitempty"`
	SearchEngine       string `json:"search_engine,omitempty"`
	Provider           string `json:"provider,omitempty"`
	Model              string `json:"model,omitempty"`
	CandidateThreshold int    `json:"candidate_threshold,omitempty"`
	DomainThreshold    int    `json:"domain_threshold,omitempty"`
	MaxCandidates      int    `json:"max_candidates,omitempty"`
	MaxSiteChecks      int    `json:"max_site_checks,omitempty"`
	TimeoutSeconds     int    `json:"timeout_seconds,omitempty"`
}

type SearchBrregDomainsResult struct {
	Status           string `json:"status"`
	WorkflowRunID    string `json:"workflow_run_id,omitempty"`
	SelectionHash    string `json:"selection_hash,omitempty"`
	RecordsSelected  int32  `json:"records_selected"`
	RecordsClaimed   int32  `json:"records_claimed"`
	RecordsCompleted int32  `json:"records_completed"`
	RecordsFailed    int32  `json:"records_failed"`
	BatchesProcessed int32  `json:"batches_processed"`
}

func SearchBrregDomains(ctx temporalworkflow.Context, input SearchBrregDomainsInput) (SearchBrregDomainsResult, error) {
	input = normalizeSearchBrregDomainsInput(input)
	ctx = brregDomainSearchActivityContext(ctx)
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)

	logger.Debug("brreg domain search workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"max_attempts", input.MaxAttempts,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"search_engine", input.SearchEngine,
		"provider", input.Provider,
		"model", input.Model,
		"candidate_threshold", input.CandidateThreshold,
		"domain_threshold", input.DomainThreshold,
		"max_candidates", input.MaxCandidates,
		"max_site_checks", input.MaxSiteChecks,
		"timeout_seconds", input.TimeoutSeconds,
		"trigger", input.Trigger,
	)

	var prepared PrepareBrregDomainSearchWorkflowResult
	if err := temporalworkflow.ExecuteActivity(ctx, prepareBrregDomainSearchWorkflowActivity, PrepareBrregDomainSearchWorkflowInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		IDs:                input.IDs,
		Filters:            input.Filters,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		MaxAttempts:        int32(input.MaxAttempts),
		Trigger:            input.Trigger,
	}).Get(ctx, &prepared); err != nil {
		return SearchBrregDomainsResult{}, errors.Wrap(err, "prepare brreg domain search workflow")
	}

	result := SearchBrregDomainsResult{
		Status:          "running",
		WorkflowRunID:   prepared.WorkflowRunID,
		SelectionHash:   prepared.SelectionHash,
		RecordsSelected: prepared.RecordsSelected,
	}
	finished := false
	defer func() {
		if finished || result.WorkflowRunID == "" {
			return
		}
		disconnectedCtx, cancel := temporalworkflow.NewDisconnectedContext(ctx)
		defer cancel()
		var failedRunning FailRunningBrregDomainSearchTasksForWorkflowResult
		if err := temporalworkflow.ExecuteActivity(disconnectedCtx, failRunningBrregDomainSearchTasksActivity, FailRunningBrregDomainSearchTasksForWorkflowInput{
			WorkflowRunID: result.WorkflowRunID,
			MaxAttempts:   int32(input.MaxAttempts),
			Error:         "domain search workflow failed before all claimed records were submitted",
		}).Get(disconnectedCtx, &failedRunning); err != nil {
			logger.Warn("brreg domain search workflow cleanup failed to release running tasks",
				"workflow_run_id", result.WorkflowRunID,
				"error", err,
			)
		} else {
			logger.Debug("brreg domain search workflow cleanup released running tasks",
				"workflow_run_id", result.WorkflowRunID,
				"failed_tasks", failedRunning.FailedTasks,
			)
		}
		if err := temporalworkflow.ExecuteActivity(disconnectedCtx, finishBrregDomainSearchWorkflowActivity, FinishBrregDomainSearchWorkflowInput{
			WorkflowRunID:    result.WorkflowRunID,
			Status:           "failed",
			RecordsSeen:      result.RecordsClaimed,
			RecordsCompleted: result.RecordsCompleted,
			RecordsFailed:    result.RecordsFailed,
			Error:            "domain search workflow failed",
		}).Get(disconnectedCtx, nil); err != nil {
			logger.Warn("brreg domain search workflow cleanup failed to finish audit run",
				"workflow_run_id", result.WorkflowRunID,
				"error", err,
			)
		}
	}()

	if prepared.RecordsSelected == 0 {
		result.Status = "drained"
		if err := finishBrregDomainSearchWorkflow(ctx, result, "succeeded", ""); err != nil {
			return result, err
		}
		finished = true
		return result, nil
	}

	for {
		var claimed ClaimBrregDomainSearchBatchResult
		logger.Debug("brreg domain search workflow claiming batch",
			"workflow_run_id", prepared.WorkflowRunID,
			"selection_hash", prepared.SelectionHash,
			"batch_size", prepared.BatchSize,
			"max_parallel_tasks", input.MaxParallelTasks,
			"lease_seconds", input.LeaseSeconds,
			"batches_processed", result.BatchesProcessed,
		)
		if err := temporalworkflow.ExecuteActivity(ctx, claimBrregDomainSearchBatchActivity, ClaimBrregDomainSearchBatchInput{
			WorkflowRunID:    prepared.WorkflowRunID,
			SelectionHash:    prepared.SelectionHash,
			BatchSize:        prepared.BatchSize,
			MaxParallelTasks: int32(input.MaxParallelTasks),
			LeaseSeconds:     int32(input.LeaseSeconds),
			MaxAttempts:      prepared.MaxAttempts,
			WorkerID:         workflowInfo.WorkflowExecution.ID,
		}).Get(ctx, &claimed); err != nil {
			return result, errors.Wrap(err, "claim brreg domain search batch")
		}
		if len(claimed.Records) == 0 {
			logger.Debug("brreg domain search workflow no more claimable records",
				"workflow_run_id", prepared.WorkflowRunID,
				"records_claimed_total", result.RecordsClaimed,
				"records_completed", result.RecordsCompleted,
				"records_failed", result.RecordsFailed,
				"batches_processed", result.BatchesProcessed,
			)
			break
		}
		result.BatchesProcessed++
		result.RecordsClaimed += int32(len(claimed.Records))

		var pages FetchBrregDomainSearchPagesResult
		serviceCtx := brregDomainSearchServiceActivityContext(ctx, input.LeaseSeconds)
		if err := temporalworkflow.ExecuteActivity(serviceCtx, fetchBrregDomainSearchPagesActivity, FetchBrregDomainSearchPagesInput{
			Records:        claimed.Records,
			SearchEngine:   input.SearchEngine,
			TimeoutSeconds: input.TimeoutSeconds,
		}).Get(serviceCtx, &pages); err != nil {
			return result, errors.Wrap(err, "fetch brreg domain search pages")
		}
		logger.Debug("brreg domain search workflow fetched pages",
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"results_count", len(pages.Results),
		)

		var analyzed AnalyzeBrregDomainSearchPagesResult
		if err := temporalworkflow.ExecuteActivity(serviceCtx, analyzeBrregDomainSearchPagesActivity, AnalyzeBrregDomainSearchPagesInput{
			Records:            claimed.Records,
			Pages:              pages.Results,
			Provider:           input.Provider,
			Model:              input.Model,
			CandidateThreshold: input.CandidateThreshold,
			MaxCandidates:      input.MaxCandidates,
			TimeoutSeconds:     input.TimeoutSeconds,
		}).Get(serviceCtx, &analyzed); err != nil {
			return result, errors.Wrap(err, "analyze brreg domain search pages")
		}
		logger.Debug("brreg domain search workflow analyzed pages",
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"results_count", len(analyzed.Results),
		)

		var siteCrawls CrawlBrregDomainCandidateSitesResult
		if err := temporalworkflow.ExecuteActivity(serviceCtx, crawlBrregDomainCandidateSitesActivity, CrawlBrregDomainCandidateSitesInput{
			Results:        analyzed.Results,
			MaxSiteChecks:  input.MaxSiteChecks,
			TimeoutSeconds: input.TimeoutSeconds,
		}).Get(serviceCtx, &siteCrawls); err != nil {
			return result, errors.Wrap(err, "crawl brreg domain candidate sites")
		}
		logger.Debug("brreg domain search workflow crawled candidate sites",
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"site_crawls_count", len(siteCrawls.Results),
		)

		var verified AnalyzeBrregDomainCandidateSitesResult
		if err := temporalworkflow.ExecuteActivity(serviceCtx, analyzeBrregDomainCandidateSitesActivity, AnalyzeBrregDomainCandidateSitesInput{
			Records:         claimed.Records,
			Results:         analyzed.Results,
			SiteCrawls:      siteCrawls.Results,
			Provider:        input.Provider,
			Model:           input.Model,
			DomainThreshold: input.DomainThreshold,
			TimeoutSeconds:  input.TimeoutSeconds,
		}).Get(serviceCtx, &verified); err != nil {
			return result, errors.Wrap(err, "analyze brreg domain candidate sites")
		}
		logger.Debug("brreg domain search workflow verified candidate sites",
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"results_count", len(verified.Results),
		)

		var submitted SubmitBrregDomainSearchResultsResult
		if err := temporalworkflow.ExecuteActivity(ctx, submitBrregDomainSearchResultsActivity, SubmitBrregDomainSearchResultsInput{
			WorkflowRunID: prepared.WorkflowRunID,
			Results:       verified.Results,
			MaxAttempts:   prepared.MaxAttempts,
		}).Get(ctx, &submitted); err != nil {
			return result, errors.Wrap(err, "submit brreg domain search results")
		}
		logger.Debug("brreg domain search workflow submitted results",
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"records_submitted", submitted.RecordsSubmitted,
			"records_completed", submitted.RecordsCompleted,
			"records_failed", submitted.RecordsFailed,
		)
		result.RecordsCompleted += submitted.RecordsCompleted
		result.RecordsFailed += submitted.RecordsFailed
	}

	result.Status = "succeeded"
	if err := finishBrregDomainSearchWorkflow(ctx, result, "succeeded", ""); err != nil {
		return result, err
	}
	finished = true
	logger.Debug("brreg domain search workflow finished",
		"workflow_run_id", result.WorkflowRunID,
		"records_selected", result.RecordsSelected,
		"records_claimed", result.RecordsClaimed,
		"records_completed", result.RecordsCompleted,
		"records_failed", result.RecordsFailed,
		"batches_processed", result.BatchesProcessed,
	)
	return result, nil
}

func normalizeSearchBrregDomainsInput(input SearchBrregDomainsInput) SearchBrregDomainsInput {
	if input.Limit <= 0 {
		input.Limit = defaultDomainSearchLimit
	}
	if input.BatchSize <= 0 {
		input.BatchSize = defaultDomainSearchBatchSize
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = defaultDomainSearchMaxAttempts
	}
	if input.MaxParallelTasks <= 0 {
		input.MaxParallelTasks = defaultDomainSearchMaxParallelTasks
	}
	if input.LeaseSeconds <= 0 {
		input.LeaseSeconds = defaultDomainSearchLeaseSeconds
	}
	if input.SearchEngine == "" {
		input.SearchEngine = "duckduckgo"
	}
	if input.CandidateThreshold <= 0 {
		input.CandidateThreshold = defaultDomainSearchCandidateThreshold
	}
	if input.DomainThreshold <= 0 {
		input.DomainThreshold = defaultDomainSearchDomainThreshold
	}
	if input.MaxCandidates <= 0 {
		input.MaxCandidates = defaultDomainSearchMaxCandidates
	}
	if input.MaxSiteChecks <= 0 {
		input.MaxSiteChecks = defaultDomainSearchMaxSiteChecks
	}
	if input.TimeoutSeconds <= 0 {
		input.TimeoutSeconds = defaultDomainSearchTimeoutSeconds
	}
	if input.Provider == "" {
		input.Provider = "default"
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func brregDomainSearchActivityContext(ctx temporalworkflow.Context) temporalworkflow.Context {
	return temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    defaultDomainSearchMutationActivityAttempts,
		},
	})
}

func brregDomainSearchServiceActivityContext(ctx temporalworkflow.Context, leaseSeconds int) temporalworkflow.Context {
	timeout := time.Duration(leaseSeconds) * time.Second
	if timeout < 10*time.Minute {
		timeout = 10 * time.Minute
	}
	return temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    defaultDomainSearchServiceActivityAttempts,
		},
	})
}

func finishBrregDomainSearchWorkflow(
	ctx temporalworkflow.Context,
	result SearchBrregDomainsResult,
	status string,
	errorMessage string,
) error {
	if err := temporalworkflow.ExecuteActivity(ctx, finishBrregDomainSearchWorkflowActivity, FinishBrregDomainSearchWorkflowInput{
		WorkflowRunID:    result.WorkflowRunID,
		Status:           status,
		RecordsSeen:      result.RecordsClaimed,
		RecordsCompleted: result.RecordsCompleted,
		RecordsFailed:    result.RecordsFailed,
		Error:            errorMessage,
	}).Get(ctx, nil); err != nil {
		return errors.Wrap(err, "finish brreg domain search workflow")
	}
	return nil
}
