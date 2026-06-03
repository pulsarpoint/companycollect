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
	env.RegisterActivityWithOptions(func(input ApplyBrregCachedCompanyTranslationsInput) (ApplyBrregCachedCompanyTranslationsResult, error) {
		require.Equal(t, "v1", input.PromptVersion)
		switch input.CompanyID {
		case "11111111-1111-1111-1111-111111111111":
			return ApplyBrregCachedCompanyTranslationsResult{FieldsSeen: 2, FieldsApplied: 2, RemainingFields: 0}, nil
		case "22222222-2222-2222-2222-222222222222":
			return ApplyBrregCachedCompanyTranslationsResult{FieldsSeen: 0, FieldsApplied: 0, RemainingFields: 0}, nil
		default:
			t.Fatalf("unexpected company id %s", input.CompanyID)
			return ApplyBrregCachedCompanyTranslationsResult{}, nil
		}
	}, activity.RegisterOptions{Name: applyBrregCachedCompanyTranslationsActivity})

	var succeeded []MarkBrregCompanyTranslationSucceededInput
	env.RegisterActivityWithOptions(func(input MarkBrregCompanyTranslationSucceededInput) error {
		succeeded = append(succeeded, input)
		return nil
	}, activity.RegisterOptions{Name: markBrregCompanyTranslationSucceededActivity})

	var skipped []MarkBrregCompanyTranslationSkippedInput
	env.RegisterActivityWithOptions(func(input MarkBrregCompanyTranslationSkippedInput) error {
		skipped = append(skipped, input)
		return nil
	}, activity.RegisterOptions{Name: markBrregCompanyTranslationSkippedActivity})

	env.RegisterActivityWithOptions(func(input MarkBrregCompanyTranslationFailedInput) error {
		t.Fatalf("unexpected failure mark for %s", input.CompanyID)
		return nil
	}, activity.RegisterOptions{Name: markBrregCompanyTranslationFailedActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, succeeded, 1)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", succeeded[0].CompanyID)
	require.Len(t, skipped, 1)
	require.Equal(t, "22222222-2222-2222-2222-222222222222", skipped[0].CompanyID)

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
	env.RegisterActivityWithOptions(func(ApplyBrregCachedCompanyTranslationsInput) (ApplyBrregCachedCompanyTranslationsResult, error) {
		return ApplyBrregCachedCompanyTranslationsResult{FieldsSeen: 3, FieldsApplied: 1, RemainingFields: 2}, nil
	}, activity.RegisterOptions{Name: applyBrregCachedCompanyTranslationsActivity})
	env.RegisterActivityWithOptions(func(MarkBrregCompanyTranslationSucceededInput) error {
		t.Fatal("unexpected succeeded mark")
		return nil
	}, activity.RegisterOptions{Name: markBrregCompanyTranslationSucceededActivity})
	env.RegisterActivityWithOptions(func(MarkBrregCompanyTranslationSkippedInput) error {
		t.Fatal("unexpected skipped mark")
		return nil
	}, activity.RegisterOptions{Name: markBrregCompanyTranslationSkippedActivity})

	var failed MarkBrregCompanyTranslationFailedInput
	env.RegisterActivityWithOptions(func(input MarkBrregCompanyTranslationFailedInput) error {
		failed = input
		return nil
	}, activity.RegisterOptions{Name: markBrregCompanyTranslationFailedActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "all company translation records failed")
	require.Equal(t, "33333333-3333-3333-3333-333333333333", failed.CompanyID)
	require.False(t, failed.Terminal)
	require.Equal(t, "translation_cache", failed.ErrorCategory)
	require.Equal(t, "missing_cached_terms", failed.ErrorCode)
	require.Equal(t, "wait_for_terms", failed.RetryStrategy)
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
