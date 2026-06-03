package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateBrregSourceCompaniesCompletesCachedCompanies(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	env.RegisterActivityWithOptions(func(input ClaimBrregCompaniesForTranslationInput) (ClaimBrregCompaniesForTranslationResult, error) {
		require.EqualValues(t, 10, input.Limit)
		require.EqualValues(t, 10, input.MaxParallelTasks)
		require.EqualValues(t, 900, input.LeaseSeconds)
		require.EqualValues(t, 5, input.MaxAttempts)
		require.NotEmpty(t, input.WorkerID)
		return ClaimBrregCompaniesForTranslationResult{
			Companies: []ClaimedCompanyForTranslation{
				{CompanyID: "11111111-1111-1111-1111-111111111111", OrganizationNumber: "999111222", OrganizationName: "CACHE TEST AS"},
				{CompanyID: "22222222-2222-2222-2222-222222222222", OrganizationNumber: "999111333", OrganizationName: "NO FIELDS TEST AS"},
			},
		}, nil
	}, activity.RegisterOptions{Name: claimBrregCompaniesForTranslationActivity})
	var processed []ProcessBrregCompanyTranslationInput
	env.RegisterActivityWithOptions(func(input ProcessBrregCompanyTranslationInput) (ProcessBrregCompanyTranslationResult, error) {
		require.Equal(t, "v1", input.PromptVersion)
		require.Equal(t, "deepseek", input.Provider)
		require.Equal(t, "deepseek-chat", input.Model)
		processed = append(processed, input)
		switch input.CompanyID {
		case "11111111-1111-1111-1111-111111111111":
			return ProcessBrregCompanyTranslationResult{CompanyID: input.CompanyID, Status: "succeeded", FieldsSeen: 2, FieldsApplied: 2, RemainingFields: 0}, nil
		case "22222222-2222-2222-2222-222222222222":
			return ProcessBrregCompanyTranslationResult{CompanyID: input.CompanyID, Status: "skipped", FieldsSeen: 0, FieldsApplied: 0, RemainingFields: 0}, nil
		default:
			t.Fatalf("unexpected company id %s", input.CompanyID)
			return ProcessBrregCompanyTranslationResult{}, nil
		}
	}, activity.RegisterOptions{Name: processBrregCompanyTranslationActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		Provider: "deepseek",
		Model:    "deepseek-chat",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, processed, 2)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", processed[0].CompanyID)
	require.Equal(t, "22222222-2222-2222-2222-222222222222", processed[1].CompanyID)

	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.CompaniesClaimed)
	require.EqualValues(t, 1, result.CompaniesSucceeded)
	require.EqualValues(t, 1, result.CompaniesSkipped)
	require.Zero(t, result.CompaniesFailed)
}

func TestTranslateBrregSourceCompaniesFailsWhenAllClaimedCompaniesMissCache(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	env.RegisterActivityWithOptions(func(ClaimBrregCompaniesForTranslationInput) (ClaimBrregCompaniesForTranslationResult, error) {
		return ClaimBrregCompaniesForTranslationResult{
			Companies: []ClaimedCompanyForTranslation{
				{CompanyID: "33333333-3333-3333-3333-333333333333", OrganizationNumber: "999111444", OrganizationName: "MISSING CACHE TEST AS"},
			},
		}, nil
	}, activity.RegisterOptions{Name: claimBrregCompaniesForTranslationActivity})
	var failed ProcessBrregCompanyTranslationInput
	env.RegisterActivityWithOptions(func(input ProcessBrregCompanyTranslationInput) (ProcessBrregCompanyTranslationResult, error) {
		failed = input
		return ProcessBrregCompanyTranslationResult{CompanyID: input.CompanyID, Status: "failed", FieldsSeen: 3, FieldsApplied: 1, RemainingFields: 2}, nil
	}, activity.RegisterOptions{Name: processBrregCompanyTranslationActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "all company translation records failed")
	require.Equal(t, "33333333-3333-3333-3333-333333333333", failed.CompanyID)
	require.EqualValues(t, 5, failed.MaxAttempts)
}

func TestTranslateBrregSourceCompaniesDrainsWhenNothingClaimed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	env.RegisterActivityWithOptions(func(ClaimBrregCompaniesForTranslationInput) (ClaimBrregCompaniesForTranslationResult, error) {
		return ClaimBrregCompaniesForTranslationResult{}, nil
	}, activity.RegisterOptions{Name: claimBrregCompaniesForTranslationActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "drained", result.Status)
	require.Zero(t, result.CompaniesClaimed)
}
