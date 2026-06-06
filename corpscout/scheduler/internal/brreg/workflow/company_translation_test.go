package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateBrregSourceCompaniesPreparesQueueAndWaitsForDrain(t *testing.T) {
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

	var statusCalls int
	statusTimes := make([]time.Time, 0, 2)
	env.RegisterActivityWithOptions(func() (GetBrregTranslationQueueStatusResult, error) {
		statusCalls++
		statusTimes = append(statusTimes, env.Now())
		if statusCalls == 1 {
			return GetBrregTranslationQueueStatusResult{Pending: 2}, nil
		}
		return GetBrregTranslationQueueStatusResult{Succeeded: 2}, nil
	}, activity.RegisterOptions{Name: getBrregTranslationQueueStatusActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		Provider:          "deepseek",
		Model:             "deepseek-chat",
		PromptVersion:     "v1",
		BatchDelaySeconds: 5,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "deepseek", buildInput.Provider)
	require.Equal(t, "deepseek-chat", buildInput.Model)
	require.Equal(t, "v1", buildInput.PromptVersion)
	require.EqualValues(t, 10, buildInput.CompanyLimit)

	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.StatusRowsInserted)
	require.EqualValues(t, 2, result.CompaniesSucceeded)
	require.EqualValues(t, 3, result.FieldsSeen)
	require.Zero(t, result.BatchesProcessed)
	require.Equal(t, 2, statusCalls)
	require.Len(t, statusTimes, 2)
	require.GreaterOrEqual(t, statusTimes[1].Sub(statusTimes[0]), 5*time.Second)
}

func TestTranslateBrregSourceCompaniesAllRecordsPreparesUnboundedQueue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		require.EqualValues(t, 0, input.CompanyLimit)
		return BuildBrregTranslationWorksetResult{
			FieldsExported:    25,
			TermsExported:     25,
			CompaniesExported: 25,
			CompaniesQueued:   25,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	env.RegisterActivityWithOptions(func() (GetBrregTranslationQueueStatusResult, error) {
		return GetBrregTranslationQueueStatusResult{}, nil
	}, activity.RegisterOptions{Name: getBrregTranslationQueueStatusActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{AllRecords: true})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 25, result.StatusRowsInserted)
	require.EqualValues(t, 25, result.FieldsSeen)
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
