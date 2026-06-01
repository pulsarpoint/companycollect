package actions

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

func TestClaimTranslationCommandFromInputMapsFields(t *testing.T) {
	workflowRunID := uuid.New()
	input := ClaimBrregTranslationBatchInput{
		WorkflowRunID:    workflowRunID.String(),
		SelectionHash:    "selection-hash",
		BatchSize:        50,
		MaxParallelTasks: 5,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         "worker-1",
		Metadata:         json.RawMessage(`{"trigger":"manual"}`),
	}

	command := claimTranslationCommandFromInput(input, workflowRunID)

	require.NotNil(t, command.WorkflowRunID)
	require.Equal(t, workflowRunID, *command.WorkflowRunID)
	require.Equal(t, "selection-hash", command.SelectionHash)
	require.Equal(t, int32(50), command.BatchSize)
	require.Equal(t, int32(5), command.MaxParallelTasks)
	require.Equal(t, int32(900), command.LeaseSeconds)
	require.Equal(t, int32(3), command.MaxAttempts)
	require.NotNil(t, command.WorkerID)
	require.Equal(t, "worker-1", *command.WorkerID)
	require.JSONEq(t, `{"trigger":"manual"}`, string(command.Metadata))
}

func TestTranslateRequestFromInputMapsRecordsAndOptions(t *testing.T) {
	request := translateRequestFromInput(TranslateBrregBatchInput{
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		SourceLang:    "no",
		TargetLang:    "en",
		MaxRetries:    2,
		Records: []ClaimedTranslationRecord{{
			RawRecordID:        "record-1",
			TaskAttemptID:      "attempt-1",
			OrganizationNumber: "810202572",
			RawPayload:         json.RawMessage(`{"navn":"BORTIGARD AS"}`),
		}},
	})

	require.Equal(t, "mock", request.LLM.Provider)
	require.Equal(t, "mock-fast", request.LLM.Model)
	require.Equal(t, "v1", request.PromptVersion)
	require.Equal(t, "no", request.SourceLang)
	require.Equal(t, "en", request.TargetLang)
	require.Equal(t, 2, request.MaxRetries)
	require.Equal(t, json.RawMessage(`{"navn":"BORTIGARD AS"}`), request.Records[0].RawPayload)
}

func TestTranslateResultFromResponseCarriesTaskAttemptIDs(t *testing.T) {
	result := translateResultFromResponse(
		[]ClaimedTranslationRecord{{
			RawRecordID:        "record-1",
			TaskAttemptID:      "attempt-1",
			OrganizationNumber: "810202572",
		}},
		translationclient.BrregTranslateResponse{
			Status:           "succeeded",
			Provider:         "mock",
			Model:            "mock-fast",
			PromptVersion:    "v1",
			RecordsSeen:      1,
			RecordsCompleted: 1,
			Results: []translationclient.BrregRecordTranslationResult{{
				RecordID:           "record-1",
				OrganizationNumber: "810202572",
				Status:             "succeeded",
				TranslatedPayload:  map[string]any{"name": "BORTIGARD AS"},
				DurationMS:         12,
			}},
		},
	)

	require.Len(t, result.Results, 1)
	require.Equal(t, "attempt-1", result.Results[0].TaskAttemptID)
	require.Equal(t, map[string]any{"name": "BORTIGARD AS"}, result.Results[0].TranslatedPayload)
}

func TestSubmitTranslationCommandFromResultMapsFailure(t *testing.T) {
	rawRecordID := uuid.New()
	taskAttemptID := uuid.New()
	command, err := submitTranslationCommandFromResult(TranslationRecordResult{
		RawRecordID:        rawRecordID.String(),
		TaskAttemptID:      taskAttemptID.String(),
		OrganizationNumber: "810202572",
		Status:             "failed",
		Error: &TranslationError{
			Message:       "translation service did not return all terms",
			Category:      "invalid_llm_output",
			Code:          "missing_translation_terms",
			RetryStrategy: "change_model_or_prompt",
		},
	}, rawRecordID, taskAttemptID, 3)

	require.NoError(t, err)
	require.Equal(t, rawRecordID, command.Result.RawRecordID)
	require.Equal(t, taskAttemptID, command.Result.TaskAttemptID)
	require.Equal(t, "failed", command.Result.Status)
	require.NotNil(t, command.Result.Error)
	require.Equal(t, "translation service did not return all terms", *command.Result.Error)
	require.NotNil(t, command.Failure)
	require.Equal(t, "invalid_llm_output", command.Failure.ErrorCategory)
	require.Equal(t, "missing_translation_terms", command.Failure.ErrorCode)
	require.Equal(t, "change_model_or_prompt", command.Failure.RetryStrategy)
	require.Equal(t, int32(3), command.MaxAttempts)
}
