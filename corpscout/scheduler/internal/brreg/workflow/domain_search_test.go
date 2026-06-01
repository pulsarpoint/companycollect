package workflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestSearchBrregDomainsFinishesWhenSelectionIsEmpty(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(SearchBrregDomains)
	env.RegisterActivityWithOptions(func(PrepareBrregDomainSearchWorkflowInput) (PrepareBrregDomainSearchWorkflowResult, error) {
		return PrepareBrregDomainSearchWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-empty",
			RecordsSelected: 0,
			BatchSize:       10,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregDomainSearchWorkflow"})
	env.RegisterActivityWithOptions(func(FinishBrregDomainSearchWorkflowInput) (FinishBrregDomainSearchWorkflowResult, error) {
		return FinishBrregDomainSearchWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregDomainSearchWorkflow"})
	cleanupCalls := 0
	registerDomainSearchLeaseCleanup(env, &cleanupCalls)

	env.ExecuteWorkflow(SearchBrregDomains, SearchBrregDomainsInput{Limit: 1000, BatchSize: 10})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result SearchBrregDomainsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "drained", result.Status)
	require.EqualValues(t, 0, result.RecordsSelected)
	require.EqualValues(t, 0, result.RecordsClaimed)
	require.Equal(t, 1, cleanupCalls)
}

func TestSearchBrregDomainsProcessesBatchesUntilClaimDrains(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(SearchBrregDomains)
	env.RegisterActivityWithOptions(func(PrepareBrregDomainSearchWorkflowInput) (PrepareBrregDomainSearchWorkflowResult, error) {
		return PrepareBrregDomainSearchWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-two-batches",
			RecordsSelected: 3,
			BatchSize:       2,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregDomainSearchWorkflow"})

	claimCalls := 0
	env.RegisterActivityWithOptions(func(ClaimBrregDomainSearchBatchInput) (ClaimBrregDomainSearchBatchResult, error) {
		claimCalls++
		switch claimCalls {
		case 1:
			return ClaimBrregDomainSearchBatchResult{Records: []ClaimedDomainSearchRecord{
				{
					RawRecordID:        "11111111-1111-1111-1111-111111111111",
					TaskAttemptID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
					OrganizationNumber: "111",
					OrganizationName:   "A AS",
					RawPayload:         []byte(`{"navn":"A AS"}`),
				},
				{
					RawRecordID:        "22222222-2222-2222-2222-222222222222",
					TaskAttemptID:      "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
					OrganizationNumber: "222",
					OrganizationName:   "B AS",
					RawPayload:         []byte(`{"navn":"B AS"}`),
				},
			}}, nil
		case 2:
			return ClaimBrregDomainSearchBatchResult{Records: []ClaimedDomainSearchRecord{
				{
					RawRecordID:        "33333333-3333-3333-3333-333333333333",
					TaskAttemptID:      "cccccccc-cccc-cccc-cccc-cccccccccccc",
					OrganizationNumber: "333",
					OrganizationName:   "C AS",
					RawPayload:         []byte(`{"navn":"C AS"}`),
				},
			}}, nil
		default:
			return ClaimBrregDomainSearchBatchResult{}, nil
		}
	}, activity.RegisterOptions{Name: "ClaimBrregDomainSearchBatch"})

	env.RegisterActivityWithOptions(func(input FetchBrregDomainSearchPagesInput) (FetchBrregDomainSearchPagesResult, error) {
		results := make([]DomainSearchPageResult, 0, len(input.Records))
		for _, record := range input.Records {
			results = append(results, DomainSearchPageResult{
				RawRecordID:        record.RawRecordID,
				TaskAttemptID:      record.TaskAttemptID,
				OrganizationNumber: record.OrganizationNumber,
				SearchEngine:       input.SearchEngine,
				SearchTerm:         record.OrganizationName + " NO website",
				Status:             "succeeded",
			})
		}
		return FetchBrregDomainSearchPagesResult{Results: results}, nil
	}, activity.RegisterOptions{Name: "FetchBrregDomainSearchPages"})

	env.RegisterActivityWithOptions(func(input AnalyzeBrregDomainSearchPagesInput) (AnalyzeBrregDomainSearchPagesResult, error) {
		results := make([]DomainSearchRecordResult, 0, len(input.Pages))
		for _, page := range input.Pages {
			results = append(results, DomainSearchRecordResult{
				RawRecordID:        page.RawRecordID,
				TaskAttemptID:      page.TaskAttemptID,
				OrganizationNumber: page.OrganizationNumber,
				SearchEngine:       page.SearchEngine,
				SearchTerm:         page.SearchTerm,
				Status:             "partial",
			})
		}
		return AnalyzeBrregDomainSearchPagesResult{Results: results}, nil
	}, activity.RegisterOptions{Name: "AnalyzeBrregDomainSearchPages"})

	env.RegisterActivityWithOptions(func(input CrawlBrregDomainCandidateSitesInput) (CrawlBrregDomainCandidateSitesResult, error) {
		return CrawlBrregDomainCandidateSitesResult{}, nil
	}, activity.RegisterOptions{Name: "CrawlBrregDomainCandidateSites"})

	env.RegisterActivityWithOptions(func(input AnalyzeBrregDomainCandidateSitesInput) (AnalyzeBrregDomainCandidateSitesResult, error) {
		return AnalyzeBrregDomainCandidateSitesResult{Results: input.Results}, nil
	}, activity.RegisterOptions{Name: "AnalyzeBrregDomainCandidateSites"})

	env.RegisterActivityWithOptions(func(input SubmitBrregDomainSearchResultsInput) (SubmitBrregDomainSearchResultsResult, error) {
		return SubmitBrregDomainSearchResultsResult{
			RecordsSubmitted: int32(len(input.Results)),
			RecordsCompleted: int32(len(input.Results)),
		}, nil
	}, activity.RegisterOptions{Name: "SubmitBrregDomainSearchResults"})
	env.RegisterActivityWithOptions(func(FinishBrregDomainSearchWorkflowInput) (FinishBrregDomainSearchWorkflowResult, error) {
		return FinishBrregDomainSearchWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregDomainSearchWorkflow"})
	cleanupCalls := 0
	registerDomainSearchLeaseCleanup(env, &cleanupCalls)

	env.ExecuteWorkflow(SearchBrregDomains, SearchBrregDomainsInput{Limit: 3, BatchSize: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result SearchBrregDomainsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 3, result.RecordsSelected)
	require.EqualValues(t, 3, result.RecordsClaimed)
	require.EqualValues(t, 3, result.RecordsCompleted)
	require.EqualValues(t, 0, result.RecordsFailed)
	require.EqualValues(t, 2, result.BatchesProcessed)
	require.Equal(t, 1, cleanupCalls)
}

func TestSearchBrregDomainsFailsWhenAllRecordsFailInBusinessStep(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(SearchBrregDomains)
	env.RegisterActivityWithOptions(func(PrepareBrregDomainSearchWorkflowInput) (PrepareBrregDomainSearchWorkflowResult, error) {
		return PrepareBrregDomainSearchWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-failed-records",
			RecordsSelected: 2,
			BatchSize:       2,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregDomainSearchWorkflow"})

	claimCalls := 0
	env.RegisterActivityWithOptions(func(ClaimBrregDomainSearchBatchInput) (ClaimBrregDomainSearchBatchResult, error) {
		claimCalls++
		if claimCalls > 1 {
			return ClaimBrregDomainSearchBatchResult{}, nil
		}
		return ClaimBrregDomainSearchBatchResult{Records: []ClaimedDomainSearchRecord{
			{
				RawRecordID:        "11111111-1111-1111-1111-111111111111",
				TaskAttemptID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				OrganizationNumber: "111",
				OrganizationName:   "A AS",
				RawPayload:         []byte(`{"navn":"A AS"}`),
			},
			{
				RawRecordID:        "22222222-2222-2222-2222-222222222222",
				TaskAttemptID:      "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
				OrganizationNumber: "222",
				OrganizationName:   "B AS",
				RawPayload:         []byte(`{"navn":"B AS"}`),
			},
		}}, nil
	}, activity.RegisterOptions{Name: "ClaimBrregDomainSearchBatch"})

	env.RegisterActivityWithOptions(func(input FetchBrregDomainSearchPagesInput) (FetchBrregDomainSearchPagesResult, error) {
		results := make([]DomainSearchPageResult, 0, len(input.Records))
		for _, record := range input.Records {
			results = append(results, DomainSearchPageResult{
				RawRecordID:        record.RawRecordID,
				TaskAttemptID:      record.TaskAttemptID,
				OrganizationNumber: record.OrganizationNumber,
				SearchEngine:       input.SearchEngine,
				SearchTerm:         record.OrganizationName + " NO website",
				Status:             "failed",
				Error: &DomainSearchError{
					Message:       "Search page fetch request failed.",
					Category:      "crawl_service",
					Code:          "search_fetch_request_failed",
					RetryStrategy: "retry_with_backoff",
					Detail:        map[string]any{"error": "request brreg search fetch over nats: nats: no responders available for request"},
				},
			})
		}
		return FetchBrregDomainSearchPagesResult{Results: results}, nil
	}, activity.RegisterOptions{Name: "FetchBrregDomainSearchPages"})

	env.RegisterActivityWithOptions(func(input AnalyzeBrregDomainSearchPagesInput) (AnalyzeBrregDomainSearchPagesResult, error) {
		results := make([]DomainSearchRecordResult, 0, len(input.Pages))
		for _, page := range input.Pages {
			results = append(results, DomainSearchRecordResult{
				RawRecordID:        page.RawRecordID,
				TaskAttemptID:      page.TaskAttemptID,
				OrganizationNumber: page.OrganizationNumber,
				SearchEngine:       page.SearchEngine,
				SearchTerm:         page.SearchTerm,
				Status:             "failed",
			})
		}
		return AnalyzeBrregDomainSearchPagesResult{Results: results}, nil
	}, activity.RegisterOptions{Name: "AnalyzeBrregDomainSearchPages"})

	env.RegisterActivityWithOptions(func(input CrawlBrregDomainCandidateSitesInput) (CrawlBrregDomainCandidateSitesResult, error) {
		return CrawlBrregDomainCandidateSitesResult{}, nil
	}, activity.RegisterOptions{Name: "CrawlBrregDomainCandidateSites"})

	env.RegisterActivityWithOptions(func(input AnalyzeBrregDomainCandidateSitesInput) (AnalyzeBrregDomainCandidateSitesResult, error) {
		return AnalyzeBrregDomainCandidateSitesResult{Results: input.Results}, nil
	}, activity.RegisterOptions{Name: "AnalyzeBrregDomainCandidateSites"})

	env.RegisterActivityWithOptions(func(input SubmitBrregDomainSearchResultsInput) (SubmitBrregDomainSearchResultsResult, error) {
		return SubmitBrregDomainSearchResultsResult{
			RecordsSubmitted: int32(len(input.Results)),
			RecordsFailed:    int32(len(input.Results)),
		}, nil
	}, activity.RegisterOptions{Name: "SubmitBrregDomainSearchResults"})
	var finishInput FinishBrregDomainSearchWorkflowInput
	env.RegisterActivityWithOptions(func(input FinishBrregDomainSearchWorkflowInput) (FinishBrregDomainSearchWorkflowResult, error) {
		finishInput = input
		return FinishBrregDomainSearchWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregDomainSearchWorkflow"})
	cleanupCalls := 0
	registerDomainSearchLeaseCleanup(env, &cleanupCalls)

	env.ExecuteWorkflow(SearchBrregDomains, SearchBrregDomainsInput{Limit: 2, BatchSize: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "all domain search records failed")
	require.ErrorContains(t, env.GetWorkflowError(), "search_fetch_request_failed")
	require.ErrorContains(t, env.GetWorkflowError(), "nats: no responders available for request")
	require.Equal(t, "failed", finishInput.Status)
	require.EqualValues(t, 2, finishInput.RecordsSeen)
	require.EqualValues(t, 0, finishInput.RecordsCompleted)
	require.EqualValues(t, 2, finishInput.RecordsFailed)
	require.Contains(t, finishInput.Error, "all domain search records failed")
	require.Contains(t, finishInput.Error, "search_fetch_request_failed")
	require.Contains(t, finishInput.Error, "nats: no responders available for request")
	require.Equal(t, 1, cleanupCalls)
}

func TestSearchBrregDomainsCleansRunningLeasesWhenActivityFails(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(SearchBrregDomains)
	env.RegisterActivityWithOptions(func(PrepareBrregDomainSearchWorkflowInput) (PrepareBrregDomainSearchWorkflowResult, error) {
		return PrepareBrregDomainSearchWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-activity-failure",
			RecordsSelected: 1,
			BatchSize:       1,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregDomainSearchWorkflow"})
	env.RegisterActivityWithOptions(func(ClaimBrregDomainSearchBatchInput) (ClaimBrregDomainSearchBatchResult, error) {
		return ClaimBrregDomainSearchBatchResult{Records: []ClaimedDomainSearchRecord{
			{
				RawRecordID:        "11111111-1111-1111-1111-111111111111",
				TaskAttemptID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				OrganizationNumber: "111",
				OrganizationName:   "A AS",
				RawPayload:         []byte(`{"navn":"A AS"}`),
			},
		}}, nil
	}, activity.RegisterOptions{Name: "ClaimBrregDomainSearchBatch"})
	env.RegisterActivityWithOptions(func(FetchBrregDomainSearchPagesInput) (FetchBrregDomainSearchPagesResult, error) {
		return FetchBrregDomainSearchPagesResult{}, errors.New("crawl service unavailable")
	}, activity.RegisterOptions{Name: "FetchBrregDomainSearchPages"})

	var cleanupInput FailRunningBrregDomainSearchTasksForWorkflowInput
	env.RegisterActivityWithOptions(func(input FailRunningBrregDomainSearchTasksForWorkflowInput) (FailRunningBrregDomainSearchTasksForWorkflowResult, error) {
		cleanupInput = input
		return FailRunningBrregDomainSearchTasksForWorkflowResult{FailedTasks: 1}, nil
	}, activity.RegisterOptions{Name: "FailRunningBrregDomainSearchTasksForWorkflow"})
	var finishInput FinishBrregDomainSearchWorkflowInput
	env.RegisterActivityWithOptions(func(input FinishBrregDomainSearchWorkflowInput) (FinishBrregDomainSearchWorkflowResult, error) {
		finishInput = input
		return FinishBrregDomainSearchWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregDomainSearchWorkflow"})

	env.ExecuteWorkflow(SearchBrregDomains, SearchBrregDomainsInput{Limit: 1, BatchSize: 1})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "fetch brreg domain search pages")
	require.Equal(t, "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c", cleanupInput.WorkflowRunID)
	require.EqualValues(t, 3, cleanupInput.MaxAttempts)
	require.Contains(t, cleanupInput.Error, "domain search workflow failed before all claimed records were submitted")
	require.Equal(t, "failed", finishInput.Status)
	require.Equal(t, "domain search workflow failed", finishInput.Error)
}

func registerDomainSearchLeaseCleanup(env *testsuite.TestWorkflowEnvironment, cleanupCalls *int) {
	env.RegisterActivityWithOptions(func(FailRunningBrregDomainSearchTasksForWorkflowInput) (FailRunningBrregDomainSearchTasksForWorkflowResult, error) {
		if cleanupCalls != nil {
			(*cleanupCalls)++
		}
		return FailRunningBrregDomainSearchTasksForWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FailRunningBrregDomainSearchTasksForWorkflow"})
}
