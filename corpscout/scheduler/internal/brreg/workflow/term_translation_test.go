package workflow

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"
)

func TestTranslateBrregSourceTermsPublishesAndAppliesUntilDrained(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceTerms)

	applyCalls := 0
	env.RegisterActivityWithOptions(func(input ApplyBrregCachedTranslationTermsInput) (ApplyBrregCachedTranslationTermsResult, error) {
		applyCalls++
		require.Equal(t, "v1", input.PromptVersion)
		require.EqualValues(t, 10000, input.Limit)
		return ApplyBrregCachedTranslationTermsResult{FieldsApplied: 2}, nil
	}, activity.RegisterOptions{Name: "ApplyBrregCachedTranslationTerms"})

	ensureCalls := 0
	env.RegisterActivityWithOptions(func(input EnsureBrregTranslationTermsInput) (EnsureBrregTranslationTermsResult, error) {
		ensureCalls++
		require.NotEmpty(t, input.TemporalWorkflowID)
		require.Equal(t, "default", input.Provider)
		require.Equal(t, "v1", input.PromptVersion)
		require.EqualValues(t, 10000, input.Limit)
		if ensureCalls == 1 {
			return EnsureBrregTranslationTermsResult{TermsInserted: 2}, nil
		}
		return EnsureBrregTranslationTermsResult{}, nil
	}, activity.RegisterOptions{Name: "EnsureBrregTranslationTerms"})

	publishCalls := 0
	env.RegisterActivityWithOptions(func(input PublishBrregTranslationTermsInput) (PublishBrregTranslationTermsResult, error) {
		publishCalls++
		require.Equal(t, "default", input.Provider)
		require.Equal(t, "v1", input.PromptVersion)
		require.EqualValues(t, 100, input.Limit)
		require.EqualValues(t, 3, input.MaxAttempts)
		if publishCalls == 1 {
			return PublishBrregTranslationTermsResult{TermsPublished: 2}, nil
		}
		return PublishBrregTranslationTermsResult{}, nil
	}, activity.RegisterOptions{Name: "PublishBrregTranslationTerms"})

	env.ExecuteWorkflow(TranslateBrregSourceTerms, TranslateBrregSourceTermsInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, applyCalls)
	require.Equal(t, 2, ensureCalls)
	require.Equal(t, 2, publishCalls)

	var result TranslateBrregSourceTermsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.TermsInserted)
	require.EqualValues(t, 2, result.TermsPublished)
	// Applying cached terms is intentionally counted once per loop. The second
	// loop gives newly returned translations a chance to update source fields.
	require.EqualValues(t, 4, result.FieldsApplied)
	require.EqualValues(t, 2, result.Loops)
}

func TestNormalizeTranslateBrregSourceTermsInputDefaultsNonPositiveAndCapsLoops(t *testing.T) {
	input := normalizeTranslateBrregSourceTermsInput(TranslateBrregSourceTermsInput{
		Limit:               -1,
		TermBatchSize:       -1,
		MaxAttempts:         -1,
		MaxLoops:            100000,
		TermsInsertedCarry:  -1,
		TermsPublishedCarry: -1,
		FieldsAppliedCarry:  -1,
		LoopsCarry:          -1,
	})

	require.Equal(t, defaultTermTranslationLimit, input.Limit)
	require.Equal(t, defaultTermTranslationBatchSize, input.TermBatchSize)
	require.Equal(t, defaultTermTranslationMaxAttempts, input.MaxAttempts)
	require.Equal(t, maxTermTranslationLoops, input.MaxLoops)
	require.Equal(t, defaultTermTranslationProvider, input.Provider)
	require.Equal(t, defaultTermTranslationPrompt, input.PromptVersion)
	require.Equal(t, defaultTermTranslationTrigger, input.Trigger)
	require.Zero(t, input.TermsInsertedCarry)
	require.Zero(t, input.TermsPublishedCarry)
	require.Zero(t, input.FieldsAppliedCarry)
	require.Zero(t, input.LoopsCarry)
}

func TestNormalizeTranslateBrregSourceTermsInputPreservesZeroLimitWhenAllRecordsSelected(t *testing.T) {
	input := normalizeTranslateBrregSourceTermsInput(TranslateBrregSourceTermsInput{
		AllRecords: true,
	})

	require.Zero(t, input.Limit)
}

func TestTranslateBrregSourceTermsContinueAsNewCarriesCounters(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceTerms)
	env.RegisterActivityWithOptions(func(ApplyBrregCachedTranslationTermsInput) (ApplyBrregCachedTranslationTermsResult, error) {
		return ApplyBrregCachedTranslationTermsResult{FieldsApplied: 3}, nil
	}, activity.RegisterOptions{Name: "ApplyBrregCachedTranslationTerms"})
	env.RegisterActivityWithOptions(func(EnsureBrregTranslationTermsInput) (EnsureBrregTranslationTermsResult, error) {
		return EnsureBrregTranslationTermsResult{TermsInserted: 5}, nil
	}, activity.RegisterOptions{Name: "EnsureBrregTranslationTerms"})
	env.RegisterActivityWithOptions(func(PublishBrregTranslationTermsInput) (PublishBrregTranslationTermsResult, error) {
		return PublishBrregTranslationTermsResult{TermsPublished: 7}, nil
	}, activity.RegisterOptions{Name: "PublishBrregTranslationTerms"})

	env.ExecuteWorkflow(TranslateBrregSourceTerms, TranslateBrregSourceTermsInput{
		MaxLoops:            1,
		TermsInsertedCarry:  11,
		TermsPublishedCarry: 13,
		FieldsAppliedCarry:  17,
		LoopsCarry:          19,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var continueAsNewErr *sdkworkflow.ContinueAsNewError
	require.True(t, stderrors.As(err, &continueAsNewErr))

	var nextInput TranslateBrregSourceTermsInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueAsNewErr.Input, &nextInput))
	require.EqualValues(t, 16, nextInput.TermsInsertedCarry)
	require.EqualValues(t, 20, nextInput.TermsPublishedCarry)
	require.EqualValues(t, 20, nextInput.FieldsAppliedCarry)
	require.EqualValues(t, 20, nextInput.LoopsCarry)
}
