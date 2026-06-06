package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateAriregisterSourceCompaniesPreparesQueueAndWaitsForDrain(t *testing.T) {
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

	var statusCalls int
	statusTimes := make([]time.Time, 0, 2)
	env.RegisterActivityWithOptions(func() (GetAriregisterTranslationQueueStatusResult, error) {
		statusCalls++
		statusTimes = append(statusTimes, env.Now())
		if statusCalls == 1 {
			return GetAriregisterTranslationQueueStatusResult{Pending: 2}, nil
		}
		return GetAriregisterTranslationQueueStatusResult{Succeeded: 2}, nil
	}, activity.RegisterOptions{Name: getAriregisterTranslationQueueStatusActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{
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

	var result TranslateAriregisterSourceCompaniesResult
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

func TestTranslateAriregisterSourceCompaniesAllRecordsPreparesUnboundedQueue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		require.EqualValues(t, 0, input.CompanyLimit)
		return BuildAriregisterTranslationWorksetResult{
			FieldsExported:    25,
			TermsExported:     25,
			CompaniesExported: 25,
			CompaniesQueued:   25,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})

	env.RegisterActivityWithOptions(func() (GetAriregisterTranslationQueueStatusResult, error) {
		return GetAriregisterTranslationQueueStatusResult{}, nil
	}, activity.RegisterOptions{Name: getAriregisterTranslationQueueStatusActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{AllRecords: true})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateAriregisterSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 25, result.StatusRowsInserted)
	require.EqualValues(t, 25, result.FieldsSeen)
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
