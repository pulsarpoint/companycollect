package workflow

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateBrregSourceCompaniesCompletesQueueBatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	var buildInput BuildBrregTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		buildInput = input
		return BuildBrregTranslationWorksetResult{
			FieldsExported:    3,
			TermsExported:     3,
			CompaniesExported: 2,
			CompaniesQueued:   2,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimCalls++
		require.Empty(t, input.Path)
		require.EqualValues(t, 6000, input.MaxRequestChars)
		require.EqualValues(t, 25, input.MaxTerms)
		require.EqualValues(t, 8, input.MaxAttempts)
		require.EqualValues(t, 3600, input.StaleRunningSeconds)
		if claimCalls == 1 {
			require.Contains(t, input.BatchID, "/batch/000001")
			return ClaimBrregTranslationWorksetBatchResult{
				Status:         "claimed",
				BatchID:        input.BatchID,
				CompanyIDs:     []string{"company-a", "company-b"},
				EstimatedChars: 32,
			}, nil
		}
		return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})

	env.RegisterActivityWithOptions(func(input TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		require.Contains(t, input.BatchID, "/batch/000001")
		require.Equal(t, []string{"company-a", "company-b"}, input.CompanyIDs)
		require.Equal(t, "deepseek", input.Provider)
		require.Equal(t, "deepseek-chat", input.Model)
		require.Equal(t, "v1", input.PromptVersion)
		return TranslateBrregTranslationWorksetBatchResult{
			CompaniesProcessed: 2,
			FieldsSeen:         3,
			TermsClaimed:       3,
			TermsSucceeded:     3,
			TermsSaved:         3,
			BindingsApplied:    3,
		}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})

	env.RegisterActivityWithOptions(func(input CompleteBrregTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		require.Contains(t, input.BatchID, "/batch/000001")
		return TranslationQueueBatchResult{RowsAffected: 2}, nil
	}, activity.RegisterOptions{Name: completeBrregTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		Provider: "deepseek",
		Model:    "deepseek-chat",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "v1", buildInput.PromptVersion)
	require.EqualValues(t, 10, buildInput.CompanyLimit)
	require.Equal(t, 2, claimCalls)

	var result TranslateBrregSourceCompaniesResult
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

func TestTranslateBrregSourceCompaniesWaitsWhenQueueIsBlocked(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	env.RegisterActivityWithOptions(func(BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{FieldsExported: 1, CompaniesExported: 1, CompaniesQueued: 1}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	claimTimes := make([]time.Time, 0, 3)
	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimTimes = append(claimTimes, env.Now())
		claimCalls++
		switch claimCalls {
		case 1:
			return ClaimBrregTranslationWorksetBatchResult{Status: "blocked"}, nil
		case 2:
			return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: input.BatchID, CompanyIDs: []string{"company-a"}, EstimatedChars: 10}, nil
		default:
			return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
		}
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})

	env.RegisterActivityWithOptions(func(TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		return TranslateBrregTranslationWorksetBatchResult{TermsClaimed: 1, TermsSucceeded: 1, TermsSaved: 1, BindingsApplied: 1}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(CompleteBrregTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		return TranslationQueueBatchResult{RowsAffected: 1}, nil
	}, activity.RegisterOptions{Name: completeBrregTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, claimTimes, 3)
	require.GreaterOrEqual(t, claimTimes[1].Sub(claimTimes[0]), 5*time.Second)
}

func TestTranslateBrregSourceCompaniesReleasesQueueBatchAfterTranslationFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	env.RegisterActivityWithOptions(func(BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{FieldsExported: 1, CompaniesExported: 1, CompaniesQueued: 1}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: input.BatchID, CompanyIDs: []string{"company-a"}}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.New("llm timeout")
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})

	var releasedBatchID string
	env.RegisterActivityWithOptions(func(input ReleaseBrregTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		releasedBatchID = input.BatchID
		return TranslationQueueBatchResult{RowsAffected: 1}, nil
	}, activity.RegisterOptions{Name: releaseBrregTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{MaxAttempts: 1})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "llm timeout")
	require.Contains(t, releasedBatchID, "/batch/000001")
}

func TestTranslateBrregSourceCompaniesAllRecordsContinuesAfterFullQueueChunk(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		require.EqualValues(t, 25, input.CompanyLimit)
		return BuildBrregTranslationWorksetResult{
			FieldsExported:    25,
			CompaniesExported: 25,
			CompaniesQueued:   25,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: input.BatchID, CompanyIDs: makeCompanyIDs(25)}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		return TranslateBrregTranslationWorksetBatchResult{TermsClaimed: 25, TermsSucceeded: 25, TermsSaved: 25, BindingsApplied: 25}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(CompleteBrregTranslationQueueBatchInput) (TranslationQueueBatchResult, error) {
		return TranslationQueueBatchResult{RowsAffected: 25}, nil
	}, activity.RegisterOptions{Name: completeBrregTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		AllRecords:           true,
		MaxCompaniesPerBatch: 25,
		MaxBatches:           1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "continue as new")
}

func TestTranslateBrregSourceCompaniesDrainsWhenNothingPrepared(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	env.RegisterActivityWithOptions(func(BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateBrregSourceCompaniesResult
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
