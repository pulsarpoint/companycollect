package workflow

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateAriregisterSourceCompaniesCachedWorksetCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)
	var buildInput BuildAriregisterTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		buildInput = input
		return BuildAriregisterTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    2,
			TermsExported:     2,
			CompaniesExported: 2,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		claimCalls++
		require.NotEmpty(t, input.Path)
		require.EqualValues(t, 12000, input.MaxRequestChars)
		require.EqualValues(t, 10, input.MaxTerms)
		require.EqualValues(t, 5, input.MaxAttempts)
		if claimCalls == 1 {
			return ClaimAriregisterTranslationWorksetBatchResult{
				Status:         "claimed",
				BatchID:        44,
				EstimatedChars: 32,
				Terms: []TranslationWorksetTerm{
					{TermKey: "term-1", SourceText: "Osaühing", SourceTextNormalized: "osaühing"},
					{TermKey: "term-2", SourceText: "Registrisse kantud", SourceTextNormalized: "registrisse kantud"},
				},
			}, nil
		}
		return ClaimAriregisterTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		require.EqualValues(t, 44, input.BatchID)
		require.Equal(t, "deepseek", input.Provider)
		require.Equal(t, "deepseek-chat", input.Model)
		require.Equal(t, "v1", input.PromptVersion)
		require.Len(t, input.Terms, 2)
		return TranslateAriregisterTranslationWorksetBatchResult{
			Results: []TranslationWorksetTermResult{
				{TermKey: "term-1", SourceText: "Osaühing", SourceTextNormalized: "osaühing", TranslatedText: "Private limited company", Status: "succeeded"},
				{TermKey: "term-2", SourceText: "Registrisse kantud", SourceTextNormalized: "registrisse kantud", TranslatedText: "Entered in the register", Status: "succeeded"},
			},
		}, nil
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input SaveAriregisterTranslationWorksetBatchInput) (SaveAriregisterTranslationWorksetBatchResult, error) {
		require.EqualValues(t, 44, input.BatchID)
		require.Len(t, input.Results, 2)
		return SaveAriregisterTranslationWorksetBatchResult{TermsSucceeded: 2}, nil
	}, activity.RegisterOptions{Name: saveAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input ApplyAriregisterTranslationWorksetInput) (ApplyAriregisterTranslationWorksetResult, error) {
		require.Equal(t, buildInput.Path, input.Path)
		return ApplyAriregisterTranslationWorksetResult{TermsSaved: 2, BindingsApplied: 2}, nil
	}, activity.RegisterOptions{Name: applyAriregisterTranslationWorksetActivity})
	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{
		Provider: "deepseek",
		Model:    "deepseek-chat",
		MaxTerms: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "v1", buildInput.PromptVersion)
	require.EqualValues(t, 10, buildInput.CompanyLimit)
	require.Contains(t, buildInput.Path, "/var/lib/corpscout/worksets/ariregister-translation")
	require.Equal(t, 2, claimCalls)

	var result TranslateAriregisterSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.CompaniesClaimed)
	require.EqualValues(t, 1, result.BatchesProcessed)
	require.EqualValues(t, 2, result.TermsClaimed)
	require.EqualValues(t, 2, result.TermsSucceeded)
	require.EqualValues(t, 2, result.FieldsSeen)
	require.EqualValues(t, 2, result.FieldsApplied)
	require.EqualValues(t, 32, result.RequestCharsClaimed)
}

func TestTranslateAriregisterSourceCompaniesPipelinesNextBatchBeforeSavingCurrent(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)
	events := make([]string, 0)
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		events = append(events, "build")
		return BuildAriregisterTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    2,
			TermsExported:     2,
			CompaniesExported: 2,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		claimCalls++
		events = append(events, "claim")
		switch claimCalls {
		case 1:
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "claimed", BatchID: 1, EstimatedChars: 10, Terms: []TranslationWorksetTerm{{TermKey: "term-1", SourceText: "Osaühing", SourceTextNormalized: "osaühing"}}}, nil
		case 2:
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "claimed", BatchID: 2, EstimatedChars: 20, Terms: []TranslationWorksetTerm{{TermKey: "term-2", SourceText: "Eesti", SourceTextNormalized: "eesti"}}}, nil
		default:
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "drained"}, nil
		}
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		events = append(events, batchEvent("translate", input.BatchID))
		return TranslateAriregisterTranslationWorksetBatchResult{Results: []TranslationWorksetTermResult{{
			TermKey:              input.Terms[0].TermKey,
			SourceText:           input.Terms[0].SourceText,
			SourceTextNormalized: input.Terms[0].SourceTextNormalized,
			TranslatedText:       "translated",
			Status:               "succeeded",
		}}}, nil
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input SaveAriregisterTranslationWorksetBatchInput) (SaveAriregisterTranslationWorksetBatchResult, error) {
		events = append(events, batchEvent("save", input.BatchID))
		return SaveAriregisterTranslationWorksetBatchResult{TermsSucceeded: int32(len(input.Results))}, nil
	}, activity.RegisterOptions{Name: saveAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyAriregisterTranslationWorksetInput) (ApplyAriregisterTranslationWorksetResult, error) {
		events = append(events, "apply")
		return ApplyAriregisterTranslationWorksetResult{TermsSaved: 2, BindingsApplied: 2}, nil
	}, activity.RegisterOptions{Name: applyAriregisterTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{MaxTerms: 10})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Less(t, indexOfEvent(events, "translate:2"), indexOfEvent(events, "save:1"), events)
}

func TestTranslateAriregisterSourceCompaniesContinuesAsNewForAllRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		return BuildAriregisterTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    25,
			TermsExported:     25,
			CompaniesExported: 25,
			CachedFields:      25,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})
	env.RegisterActivityWithOptions(func(ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		return ClaimAriregisterTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyAriregisterTranslationWorksetInput) (ApplyAriregisterTranslationWorksetResult, error) {
		return ApplyAriregisterTranslationWorksetResult{TermsSaved: 25, BindingsApplied: 25}, nil
	}, activity.RegisterOptions{Name: applyAriregisterTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{
		AllRecords:           true,
		MaxCompaniesPerBatch: 25,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "continue as new")
}

func TestTranslateAriregisterSourceCompaniesFailsWhenAllTermsFail(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		return BuildAriregisterTranslationWorksetResult{Path: input.Path, FieldsExported: 1, TermsExported: 1, CompaniesExported: 1}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(ClaimAriregisterTranslationWorksetBatchInput) (ClaimAriregisterTranslationWorksetBatchResult, error) {
		claimCalls++
		if claimCalls == 1 {
			return ClaimAriregisterTranslationWorksetBatchResult{Status: "claimed", BatchID: 1, Terms: []TranslationWorksetTerm{{TermKey: "term-1", SourceText: "Osaühing", SourceTextNormalized: "osaühing"}}}, nil
		}
		return ClaimAriregisterTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(TranslateAriregisterTranslationWorksetBatchInput) (TranslateAriregisterTranslationWorksetBatchResult, error) {
		return TranslateAriregisterTranslationWorksetBatchResult{Results: []TranslationWorksetTermResult{{TermKey: "term-1", Status: "failed_retryable", Error: "temporary"}}}, nil
	}, activity.RegisterOptions{Name: translateAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(SaveAriregisterTranslationWorksetBatchInput) (SaveAriregisterTranslationWorksetBatchResult, error) {
		return SaveAriregisterTranslationWorksetBatchResult{TermsFailed: 1}, nil
	}, activity.RegisterOptions{Name: saveAriregisterTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyAriregisterTranslationWorksetInput) (ApplyAriregisterTranslationWorksetResult, error) {
		return ApplyAriregisterTranslationWorksetResult{}, nil
	}, activity.RegisterOptions{Name: applyAriregisterTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "all company translation terms failed")
}

func TestTranslateAriregisterSourceCompaniesDrainsWhenNothingClaimed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		return BuildAriregisterTranslationWorksetResult{Path: input.Path}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateAriregisterSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "drained", result.Status)
	require.Zero(t, result.CompaniesClaimed)
}

func indexOfEvent(events []string, target string) int {
	for index, event := range events {
		if event == target {
			return index
		}
	}
	return len(events)
}

func batchEvent(prefix string, batchID int64) string {
	return prefix + ":" + strconv.FormatInt(batchID, 10)
}
