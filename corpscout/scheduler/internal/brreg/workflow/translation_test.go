package workflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateBrregRawInputsFinishesWhenSelectionIsEmpty(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregRawInputs)
	env.RegisterActivityWithOptions(func(PrepareBrregTranslationWorkflowInput) (PrepareBrregTranslationWorkflowResult, error) {
		return PrepareBrregTranslationWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-empty",
			RecordsSelected: 0,
			BatchSize:       50,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregTranslationWorkflow"})
	env.RegisterActivityWithOptions(func(FinishBrregTranslationWorkflowInput) (FinishBrregTranslationWorkflowResult, error) {
		return FinishBrregTranslationWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregTranslationWorkflow"})
	cleanupCalls := 0
	registerTranslationLeaseCleanup(env, &cleanupCalls)

	env.ExecuteWorkflow(TranslateBrregRawInputs, TranslateBrregRawInputsInput{Limit: 1000, BatchSize: 50})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result TranslateBrregRawInputsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "drained", result.Status)
	require.EqualValues(t, 0, result.RecordsSelected)
	require.EqualValues(t, 0, result.RecordsClaimed)
	require.Equal(t, 1, cleanupCalls)
}

func TestTranslateBrregRawInputsProcessesBatchesUntilClaimDrains(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregRawInputs)
	env.RegisterActivityWithOptions(func(PrepareBrregTranslationWorkflowInput) (PrepareBrregTranslationWorkflowResult, error) {
		return PrepareBrregTranslationWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-two-batches",
			RecordsSelected: 3,
			BatchSize:       2,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregTranslationWorkflow"})

	claimCalls := 0
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationBatchInput) (ClaimBrregTranslationBatchResult, error) {
		claimCalls++
		switch claimCalls {
		case 1:
			return ClaimBrregTranslationBatchResult{Records: []ClaimedTranslationRecord{
				{
					RawRecordID:        "11111111-1111-1111-1111-111111111111",
					TaskAttemptID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
					OrganizationNumber: "111",
					RawPayload:         []byte(`{"navn":"A"}`),
				},
				{
					RawRecordID:        "22222222-2222-2222-2222-222222222222",
					TaskAttemptID:      "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
					OrganizationNumber: "222",
					RawPayload:         []byte(`{"navn":"B"}`),
				},
			}}, nil
		case 2:
			return ClaimBrregTranslationBatchResult{Records: []ClaimedTranslationRecord{
				{
					RawRecordID:        "33333333-3333-3333-3333-333333333333",
					TaskAttemptID:      "cccccccc-cccc-cccc-cccc-cccccccccccc",
					OrganizationNumber: "333",
					RawPayload:         []byte(`{"navn":"C"}`),
				},
			}}, nil
		default:
			return ClaimBrregTranslationBatchResult{}, nil
		}
	}, activity.RegisterOptions{Name: "ClaimBrregTranslationBatch"})

	env.RegisterActivityWithOptions(func(input TranslateBrregBatchInput) (TranslateBrregBatchResult, error) {
		results := make([]TranslationRecordResult, 0, len(input.Records))
		for _, record := range input.Records {
			results = append(results, TranslationRecordResult{
				RawRecordID:        record.RawRecordID,
				TaskAttemptID:      record.TaskAttemptID,
				OrganizationNumber: record.OrganizationNumber,
				Status:             "succeeded",
				TranslatedPayload:  map[string]any{"name": "translated"},
				Provider:           input.Provider,
				Model:              input.Model,
				PromptVersion:      "v1",
			})
		}
		return TranslateBrregBatchResult{
			Status:           "succeeded",
			RecordsSeen:      len(input.Records),
			RecordsCompleted: len(input.Records),
			Results:          results,
		}, nil
	}, activity.RegisterOptions{Name: "TranslateBrregBatch"})

	env.RegisterActivityWithOptions(func(input SubmitBrregTranslationBatchInput) (SubmitBrregTranslationBatchResult, error) {
		return SubmitBrregTranslationBatchResult{
			RecordsSubmitted: int32(len(input.Results)),
			RecordsCompleted: int32(len(input.Results)),
		}, nil
	}, activity.RegisterOptions{Name: "SubmitBrregTranslationBatch"})
	env.RegisterActivityWithOptions(func(FinishBrregTranslationWorkflowInput) (FinishBrregTranslationWorkflowResult, error) {
		return FinishBrregTranslationWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregTranslationWorkflow"})
	cleanupCalls := 0
	registerTranslationLeaseCleanup(env, &cleanupCalls)

	env.ExecuteWorkflow(TranslateBrregRawInputs, TranslateBrregRawInputsInput{Limit: 3, BatchSize: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result TranslateBrregRawInputsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 3, result.RecordsSelected)
	require.EqualValues(t, 3, result.RecordsClaimed)
	require.EqualValues(t, 3, result.RecordsCompleted)
	require.EqualValues(t, 0, result.RecordsFailed)
	require.EqualValues(t, 2, result.BatchesProcessed)
	require.Equal(t, 1, cleanupCalls)
}

func TestTranslateBrregRawInputsFailsWhenAllRecordsFailInBusinessStep(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregRawInputs)
	env.RegisterActivityWithOptions(func(PrepareBrregTranslationWorkflowInput) (PrepareBrregTranslationWorkflowResult, error) {
		return PrepareBrregTranslationWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-failed-records",
			RecordsSelected: 2,
			BatchSize:       2,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregTranslationWorkflow"})

	claimCalls := 0
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationBatchInput) (ClaimBrregTranslationBatchResult, error) {
		claimCalls++
		if claimCalls > 1 {
			return ClaimBrregTranslationBatchResult{}, nil
		}
		return ClaimBrregTranslationBatchResult{Records: []ClaimedTranslationRecord{
			{
				RawRecordID:        "11111111-1111-1111-1111-111111111111",
				TaskAttemptID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				OrganizationNumber: "111",
				RawPayload:         []byte(`{"navn":"A"}`),
			},
			{
				RawRecordID:        "22222222-2222-2222-2222-222222222222",
				TaskAttemptID:      "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
				OrganizationNumber: "222",
				RawPayload:         []byte(`{"navn":"B"}`),
			},
		}}, nil
	}, activity.RegisterOptions{Name: "ClaimBrregTranslationBatch"})

	env.RegisterActivityWithOptions(func(input TranslateBrregBatchInput) (TranslateBrregBatchResult, error) {
		results := make([]TranslationRecordResult, 0, len(input.Records))
		for _, record := range input.Records {
			results = append(results, TranslationRecordResult{
				RawRecordID:        record.RawRecordID,
				TaskAttemptID:      record.TaskAttemptID,
				OrganizationNumber: record.OrganizationNumber,
				Status:             "failed",
				Error: &TranslationError{
					Message:       "translation service failed",
					Category:      "translation_service",
					Code:          "translation_failed",
					RetryStrategy: "retry_with_backoff",
				},
			})
		}
		return TranslateBrregBatchResult{
			Status:        "failed",
			RecordsSeen:   len(input.Records),
			RecordsFailed: len(input.Records),
			Results:       results,
		}, nil
	}, activity.RegisterOptions{Name: "TranslateBrregBatch"})

	env.RegisterActivityWithOptions(func(input SubmitBrregTranslationBatchInput) (SubmitBrregTranslationBatchResult, error) {
		return SubmitBrregTranslationBatchResult{
			RecordsSubmitted: int32(len(input.Results)),
			RecordsFailed:    int32(len(input.Results)),
		}, nil
	}, activity.RegisterOptions{Name: "SubmitBrregTranslationBatch"})
	var finishInput FinishBrregTranslationWorkflowInput
	env.RegisterActivityWithOptions(func(input FinishBrregTranslationWorkflowInput) (FinishBrregTranslationWorkflowResult, error) {
		finishInput = input
		return FinishBrregTranslationWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregTranslationWorkflow"})
	cleanupCalls := 0
	registerTranslationLeaseCleanup(env, &cleanupCalls)

	env.ExecuteWorkflow(TranslateBrregRawInputs, TranslateBrregRawInputsInput{Limit: 2, BatchSize: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "all translation records failed")
	require.Equal(t, "failed", finishInput.Status)
	require.EqualValues(t, 2, finishInput.RecordsSeen)
	require.EqualValues(t, 0, finishInput.RecordsCompleted)
	require.EqualValues(t, 2, finishInput.RecordsFailed)
	require.Equal(t, "all translation records failed", finishInput.Error)
	require.Equal(t, 1, cleanupCalls)
}

func TestTranslateBrregRawInputsCleansRunningLeasesWhenActivityFails(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(TranslateBrregRawInputs)
	env.RegisterActivityWithOptions(func(PrepareBrregTranslationWorkflowInput) (PrepareBrregTranslationWorkflowResult, error) {
		return PrepareBrregTranslationWorkflowResult{
			WorkflowRunID:   "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c",
			SelectionHash:   "selection-activity-failure",
			RecordsSelected: 1,
			BatchSize:       1,
			MaxAttempts:     3,
		}, nil
	}, activity.RegisterOptions{Name: "PrepareBrregTranslationWorkflow"})
	env.RegisterActivityWithOptions(func(ClaimBrregTranslationBatchInput) (ClaimBrregTranslationBatchResult, error) {
		return ClaimBrregTranslationBatchResult{Records: []ClaimedTranslationRecord{
			{
				RawRecordID:        "11111111-1111-1111-1111-111111111111",
				TaskAttemptID:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				OrganizationNumber: "111",
				RawPayload:         []byte(`{"navn":"A"}`),
			},
		}}, nil
	}, activity.RegisterOptions{Name: "ClaimBrregTranslationBatch"})
	env.RegisterActivityWithOptions(func(TranslateBrregBatchInput) (TranslateBrregBatchResult, error) {
		return TranslateBrregBatchResult{}, errors.New("translation service unavailable")
	}, activity.RegisterOptions{Name: "TranslateBrregBatch"})

	var cleanupInput FailRunningBrregTranslationTasksForWorkflowInput
	env.RegisterActivityWithOptions(func(input FailRunningBrregTranslationTasksForWorkflowInput) (FailRunningBrregTranslationTasksForWorkflowResult, error) {
		cleanupInput = input
		return FailRunningBrregTranslationTasksForWorkflowResult{FailedTasks: 1}, nil
	}, activity.RegisterOptions{Name: "FailRunningBrregTranslationTasksForWorkflow"})
	var finishInput FinishBrregTranslationWorkflowInput
	env.RegisterActivityWithOptions(func(input FinishBrregTranslationWorkflowInput) (FinishBrregTranslationWorkflowResult, error) {
		finishInput = input
		return FinishBrregTranslationWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FinishBrregTranslationWorkflow"})

	env.ExecuteWorkflow(TranslateBrregRawInputs, TranslateBrregRawInputsInput{Limit: 1, BatchSize: 1})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "translate brreg batch")
	require.Equal(t, "9f03a113-0c1f-495e-98b8-bbc2dedc1d4c", cleanupInput.WorkflowRunID)
	require.EqualValues(t, 3, cleanupInput.MaxAttempts)
	require.Contains(t, cleanupInput.Error, "translation workflow failed before all claimed records were submitted")
	require.Equal(t, "failed", finishInput.Status)
	require.Equal(t, "translation workflow failed", finishInput.Error)
}

func registerTranslationLeaseCleanup(env *testsuite.TestWorkflowEnvironment, cleanupCalls *int) {
	env.RegisterActivityWithOptions(func(FailRunningBrregTranslationTasksForWorkflowInput) (FailRunningBrregTranslationTasksForWorkflowResult, error) {
		if cleanupCalls != nil {
			(*cleanupCalls)++
		}
		return FailRunningBrregTranslationTasksForWorkflowResult{}, nil
	}, activity.RegisterOptions{Name: "FailRunningBrregTranslationTasksForWorkflow"})
}
