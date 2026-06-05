package workflow

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateAriregisterSourceCompaniesCompletesQueueBatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	var buildInput BuildAriregisterTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		buildInput = input
		return BuildAriregisterTranslationWorksetResult{
			FieldsExported:    3,
			TermsExported:     3,
			CompaniesExported: 2,
			CompaniesQueued:   2,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})

	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		claimCalls++
		require.Empty(t, input.Path)
		require.EqualValues(t, 6000, input.MaxRequestChars)
		require.EqualValues(t, 25, input.MaxTerms)
		require.EqualValues(t, 8, input.MaxAttempts)
		require.EqualValues(t, 3600, input.StaleRunningSeconds)
		if claimCalls == 1 {
			require.Contains(t, input.BatchID, "/batch/000001")
			return ClaimAriregisterTranslationWorksetBatchResult{
				Status:         "claimed",
				BatchID:        input.BatchID,
				CompanyIDs:     []string{"company-a", "company-b"},
				EstimatedChars: 32,
			}, nil
		}
		return ClaimAriregisterTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})

	env.RegisterActivityWithOptions(func(input TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		require.Contains(t, input.BatchID, "/batch/000001")
		require.Equal(t, []string{"company-a", "company-b"}, input.CompanyIDs)
		require.Equal(t, "deepseek", input.Provider)
		require.Equal(t, "deepseek-chat", input.Model)
		require.Equal(t, "v1", input.PromptVersion)
		return TranslateAriregisterTranslationWorksetBatchResult{
			CompaniesProcessed: 2,
			FieldsSeen:         3,
			TermsClaimed:       3,
			TermsSucceeded:     3,
			TermsSaved:         3,
			BindingsApplied:    3,
		}, nil
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})

	env.RegisterActivityWithOptions(func(input CompleteAriregisterTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		require.Contains(t, input.BatchID, "/batch/000001")
		return TranslationQueueBatchResult{RowsAffected: 2}, nil
	}, activity.RegisterOptions{Name: completeAriregisterTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{
		Provider: "deepseek",
		Model:    "deepseek-chat",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "v1", buildInput.PromptVersion)
	require.EqualValues(t, 10, buildInput.CompanyLimit)
	require.Equal(t, 2, claimCalls)

	var result TranslateAriregisterSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.StatusRowsInserted)
	require.EqualValues(t, 2, result.CompaniesClaimed)
	require.EqualValues(t, 2, result.CompaniesSucceeded)
	require.EqualValues(t, 1, result.BatchesProcessed)
	require.EqualValues(t, 3, result.TermsClaimed)
	require.EqualValues(t, 3, result.TermsSucceeded)
	require.EqualValues(t, 3, result.TermsSaved)
	require.EqualValues(t, 3, result.FieldsSeen)
	require.EqualValues(t, 3, result.FieldsApplied)
	require.EqualValues(t, 32, result.RequestCharsClaimed)
}

func TestTranslateAriregisterSourceCompaniesWaitsWhenQueueIsBlocked(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	env.RegisterActivityWithOptions(func(BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		return BuildAriregisterTranslationWorksetResult{FieldsExported: 1, CompaniesExported: 1, CompaniesQueued: 1}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})

	claimTimes := make([]time.Time, 0, 3)
	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		claimTimes = append(claimTimes, env.Now())
		claimCalls++
		switch claimCalls {
		case 1:
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "blocked"}, nil
		case 2:
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "claimed", BatchID: input.BatchID, CompanyIDs: []string{"company-a"}, EstimatedChars: 10}, nil
		default:
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "drained"}, nil
		}
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})

	env.RegisterActivityWithOptions(func(TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		return TranslateAriregisterTranslationWorksetBatchResult{TermsClaimed: 1, TermsSucceeded: 1, TermsSaved: 1, BindingsApplied: 1}, nil
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(CompleteAriregisterTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		return TranslationQueueBatchResult{RowsAffected: 1}, nil
	}, activity.RegisterOptions{Name: completeAriregisterTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, claimTimes, 3)
	require.GreaterOrEqual(t, claimTimes[1].Sub(claimTimes[0]), 5*time.Second)
}

func TestTranslateAriregisterSourceCompaniesReleasesQueueBatchAfterTranslationFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	env.RegisterActivityWithOptions(func(BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		return BuildAriregisterTranslationWorksetResult{FieldsExported: 1, CompaniesExported: 1, CompaniesQueued: 1}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})
	env.RegisterActivityWithOptions(func(input ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		return ClaimAriregisterTranslationWorksetBatchResult{Status: "claimed", BatchID: input.BatchID, CompanyIDs: []string{"company-a"}}, nil
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		return TranslateAriregisterTranslationWorksetBatchResult{}, errors.New("llm timeout")
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})

	var releasedBatchID string
	env.RegisterActivityWithOptions(func(input ReleaseAriregisterTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		releasedBatchID = input.BatchID
		return TranslationQueueBatchResult{RowsAffected: 1}, nil
	}, activity.RegisterOptions{Name: releaseAriregisterTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{MaxAttempts: 1})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "llm timeout")
	require.Contains(t, releasedBatchID, "/batch/000001")
}

func TestTranslateAriregisterSourceCompaniesAllRecordsContinuesAfterFullQueueChunk(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		require.EqualValues(t, 25, input.CompanyLimit)
		return BuildAriregisterTranslationWorksetResult{
			FieldsExported:    25,
			CompaniesExported: 25,
			CompaniesQueued:   25,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})
	env.RegisterActivityWithOptions(func(input ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		return ClaimAriregisterTranslationWorksetBatchResult{Status: "claimed", BatchID: input.BatchID, CompanyIDs: makeCompanyIDs(25)}, nil
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		return TranslateAriregisterTranslationWorksetBatchResult{TermsClaimed: 25, TermsSucceeded: 25, TermsSaved: 25, BindingsApplied: 25}, nil
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(CompleteAriregisterTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		return TranslationQueueBatchResult{RowsAffected: 25}, nil
	}, activity.RegisterOptions{Name: completeAriregisterTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{
		AllRecords:           true,
		MaxCompaniesPerBatch: 25,
		MaxBatches:           1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "continue as new")
}

func TestTranslateAriregisterSourceCompaniesDrainsWhenNothingPrepared(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	env.RegisterActivityWithOptions(func(BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		return BuildAriregisterTranslationWorksetResult{}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateAriregisterSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "drained", result.Status)
	require.Zero(t, result.CompaniesClaimed)
}

func makeCompanyIDs(count int) []string {
	ids := make([]string, 0, count)
	for index := 0; index < count; index++ {
		ids = append(ids, "company-"+string(rune('a'+index%26)))
	}
	return ids
}
