package workflow

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateBrregSourceCompaniesCompletesCachedCompanies(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	var buildInput BuildBrregTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		buildInput = input
		return BuildBrregTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    2,
			TermsExported:     2,
			CompaniesExported: 2,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimCalls++
		require.NotEmpty(t, input.Path)
		require.EqualValues(t, 12000, input.MaxRequestChars)
		require.EqualValues(t, 10, input.MaxTerms)
		require.EqualValues(t, 5, input.MaxAttempts)
		if claimCalls == 1 {
			return ClaimBrregTranslationWorksetBatchResult{
				Status:         "claimed",
				BatchID:        44,
				EstimatedChars: 32,
				Terms: []TranslationWorksetTerm{
					{TermKey: "term-1", SourceText: "Enhet", SourceTextNormalized: "enhet"},
					{TermKey: "term-2", SourceText: "Aksjeselskap", SourceTextNormalized: "aksjeselskap"},
				},
			}, nil
		}
		return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		require.EqualValues(t, 44, input.BatchID)
		require.Equal(t, "deepseek", input.Provider)
		require.Equal(t, "deepseek-chat", input.Model)
		require.Equal(t, "v1", input.PromptVersion)
		require.Len(t, input.Terms, 2)
		return TranslateBrregTranslationWorksetBatchResult{
			Results: []TranslationWorksetTermResult{
				{TermKey: "term-1", SourceText: "Enhet", SourceTextNormalized: "enhet", TranslatedText: "Entity", Status: "succeeded"},
				{TermKey: "term-2", SourceText: "Aksjeselskap", SourceTextNormalized: "aksjeselskap", TranslatedText: "Limited liability company", Status: "succeeded"},
			},
		}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input SaveBrregTranslationWorksetBatchInput) (SaveBrregTranslationWorksetBatchResult, error) {
		require.EqualValues(t, 44, input.BatchID)
		require.Len(t, input.Results, 2)
		return SaveBrregTranslationWorksetBatchResult{TermsSucceeded: 2}, nil
	}, activity.RegisterOptions{Name: saveBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input ApplyBrregTranslationWorksetInput) (ApplyBrregTranslationWorksetResult, error) {
		require.Equal(t, buildInput.Path, input.Path)
		return ApplyBrregTranslationWorksetResult{TermsSaved: 2, BindingsApplied: 2}, nil
	}, activity.RegisterOptions{Name: applyBrregTranslationWorksetActivity})
	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		Provider: "deepseek",
		Model:    "deepseek-chat",
		MaxTerms: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "v1", buildInput.PromptVersion)
	require.EqualValues(t, 10, buildInput.CompanyLimit)
	require.Contains(t, buildInput.Path, "/var/lib/corpscout/worksets/brreg-translation")
	require.Equal(t, 2, claimCalls)

	var result TranslateBrregSourceCompaniesResult
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

func TestTranslateBrregSourceCompaniesAllRecordsClaimsUntilDrained(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	var buildInput BuildBrregTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		buildInput = input
		return BuildBrregTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    3,
			TermsExported:     3,
			CompaniesExported: 3,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimCalls++
		require.EqualValues(t, 12000, input.MaxRequestChars)
		require.EqualValues(t, 10, input.MaxTerms)
		switch claimCalls {
		case 1:
			return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: 1, EstimatedChars: 12, Terms: []TranslationWorksetTerm{{TermKey: "term-1", SourceText: "Enhet", SourceTextNormalized: "enhet"}}}, nil
		case 2:
			return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: 2, EstimatedChars: 20, Terms: []TranslationWorksetTerm{{TermKey: "term-2", SourceText: "Aksjeselskap", SourceTextNormalized: "aksjeselskap"}, {TermKey: "term-3", SourceText: "Aksjekapital", SourceTextNormalized: "aksjekapital"}}}, nil
		default:
			return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
		}
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		results := make([]TranslationWorksetTermResult, 0, len(input.Terms))
		for _, term := range input.Terms {
			results = append(results, TranslationWorksetTermResult{TermKey: term.TermKey, SourceText: term.SourceText, SourceTextNormalized: term.SourceTextNormalized, TranslatedText: "translated", Status: "succeeded"})
		}
		return TranslateBrregTranslationWorksetBatchResult{Results: results}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input SaveBrregTranslationWorksetBatchInput) (SaveBrregTranslationWorksetBatchResult, error) {
		return SaveBrregTranslationWorksetBatchResult{TermsSucceeded: int32(len(input.Results))}, nil
	}, activity.RegisterOptions{Name: saveBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyBrregTranslationWorksetInput) (ApplyBrregTranslationWorksetResult, error) {
		return ApplyBrregTranslationWorksetResult{TermsSaved: 3, BindingsApplied: 3}, nil
	}, activity.RegisterOptions{Name: applyBrregTranslationWorksetActivity})
	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{AllRecords: true, MaxTerms: 10})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, defaultCompanyTranslationMaxCompanies, buildInput.CompanyLimit)
	require.Equal(t, 3, claimCalls)

	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 3, result.CompaniesClaimed)
	require.EqualValues(t, 3, result.CompaniesSucceeded)
	require.EqualValues(t, 3, result.FieldsSeen)
	require.EqualValues(t, 3, result.FieldsApplied)
	require.EqualValues(t, 2, result.BatchesProcessed)
	require.EqualValues(t, 3, result.TermsClaimed)
	require.EqualValues(t, 3, result.TermsSucceeded)
}

func TestTranslateBrregSourceCompaniesStartsNextTranslationBeforeSavingPreviousBatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	events := make([]string, 0)
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		events = append(events, "build")
		return BuildBrregTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    2,
			TermsExported:     2,
			CompaniesExported: 2,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimCalls++
		events = append(events, "claim")
		switch claimCalls {
		case 1:
			return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: 1, EstimatedChars: 10, Terms: []TranslationWorksetTerm{{TermKey: "term-1", SourceText: "Enhet", SourceTextNormalized: "enhet"}}}, nil
		case 2:
			return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: 2, EstimatedChars: 20, Terms: []TranslationWorksetTerm{{TermKey: "term-2", SourceText: "Aksjeselskap", SourceTextNormalized: "aksjeselskap"}}}, nil
		default:
			return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
		}
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		events = append(events, batchEvent("translate", input.BatchID))
		return TranslateBrregTranslationWorksetBatchResult{Results: []TranslationWorksetTermResult{{
			TermKey:              input.Terms[0].TermKey,
			SourceText:           input.Terms[0].SourceText,
			SourceTextNormalized: input.Terms[0].SourceTextNormalized,
			TranslatedText:       "translated",
			Status:               "succeeded",
		}}}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input SaveBrregTranslationWorksetBatchInput) (SaveBrregTranslationWorksetBatchResult, error) {
		events = append(events, batchEvent("save", input.BatchID))
		return SaveBrregTranslationWorksetBatchResult{TermsSucceeded: int32(len(input.Results))}, nil
	}, activity.RegisterOptions{Name: saveBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyBrregTranslationWorksetInput) (ApplyBrregTranslationWorksetResult, error) {
		events = append(events, "apply")
		return ApplyBrregTranslationWorksetResult{TermsSaved: 2, BindingsApplied: 2}, nil
	}, activity.RegisterOptions{Name: applyBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{MaxTerms: 10})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Less(t, indexOfEvent(events, "translate:2"), indexOfEvent(events, "save:1"), events)
}

func TestTranslateBrregSourceCompaniesDoesNotPipelineAcrossContinueAsNewBoundary(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	events := make([]string, 0)
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    2,
			TermsExported:     2,
			CompaniesExported: 2,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimCalls++
		events = append(events, "claim")
		return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: int64(claimCalls), EstimatedChars: 10, Terms: []TranslationWorksetTerm{{TermKey: "term", SourceText: "Enhet", SourceTextNormalized: "enhet"}}}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		events = append(events, batchEvent("translate", input.BatchID))
		return TranslateBrregTranslationWorksetBatchResult{Results: []TranslationWorksetTermResult{{
			TermKey:              input.Terms[0].TermKey,
			SourceText:           input.Terms[0].SourceText,
			SourceTextNormalized: input.Terms[0].SourceTextNormalized,
			TranslatedText:       "translated",
			Status:               "succeeded",
		}}}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(input SaveBrregTranslationWorksetBatchInput) (SaveBrregTranslationWorksetBatchResult, error) {
		events = append(events, batchEvent("save", input.BatchID))
		return SaveBrregTranslationWorksetBatchResult{TermsSucceeded: 1}, nil
	}, activity.RegisterOptions{Name: saveBrregTranslationWorksetBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{MaxBatches: 1})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "continue as new")
	require.Equal(t, []string{"claim", "translate:1", "save:1"}, events)
}

func TestTranslateBrregSourceCompaniesAllRecordsBuildsBoundedWorkset(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	var buildInput BuildBrregTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		buildInput = input
		return BuildBrregTranslationWorksetResult{Path: input.Path}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		AllRecords:           true,
		MaxCompaniesPerBatch: 25,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 25, buildInput.CompanyLimit)
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

func TestTranslateBrregSourceCompaniesAllRecordsContinuesAfterFullCachedWorkset(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{
			Path:              input.Path,
			FieldsExported:    25,
			TermsExported:     25,
			CompaniesExported: 25,
			CachedFields:      25,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	env.RegisterActivityWithOptions(func(ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyBrregTranslationWorksetInput) (ApplyBrregTranslationWorksetResult, error) {
		return ApplyBrregTranslationWorksetResult{TermsSaved: 25, BindingsApplied: 25}, nil
	}, activity.RegisterOptions{Name: applyBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		AllRecords:           true,
		MaxCompaniesPerBatch: 25,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "continue as new")
}

func TestTranslateBrregSourceCompaniesFailsWhenAllClaimedCompaniesMissCache(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{Path: input.Path, FieldsExported: 1, TermsExported: 1, CompaniesExported: 1}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})
	var claimCalls int
	env.RegisterActivityWithOptions(func(ClaimBrregTranslationWorksetBatchInput) (ClaimBrregTranslationWorksetBatchResult, error) {
		claimCalls++
		if claimCalls == 1 {
			return ClaimBrregTranslationWorksetBatchResult{Status: "claimed", BatchID: 1, Terms: []TranslationWorksetTerm{{TermKey: "term-1", SourceText: "Enhet", SourceTextNormalized: "enhet"}}}, nil
		}
		return ClaimBrregTranslationWorksetBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(TranslateBrregTranslationWorksetBatchInput) (TranslateBrregTranslationWorksetBatchResult, error) {
		return TranslateBrregTranslationWorksetBatchResult{Results: []TranslationWorksetTermResult{{TermKey: "term-1", Status: "failed_retryable", Error: "temporary"}}}, nil
	}, activity.RegisterOptions{Name: translateBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(SaveBrregTranslationWorksetBatchInput) (SaveBrregTranslationWorksetBatchResult, error) {
		return SaveBrregTranslationWorksetBatchResult{TermsFailed: 1}, nil
	}, activity.RegisterOptions{Name: saveBrregTranslationWorksetBatchActivity})
	env.RegisterActivityWithOptions(func(ApplyBrregTranslationWorksetInput) (ApplyBrregTranslationWorksetResult, error) {
		return ApplyBrregTranslationWorksetResult{}, nil
	}, activity.RegisterOptions{Name: applyBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "all company translation terms failed")
}

func TestTranslateBrregSourceCompaniesDrainsWhenNothingClaimed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		return BuildBrregTranslationWorksetResult{Path: input.Path}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "drained", result.Status)
	require.Zero(t, result.CompaniesClaimed)
}
