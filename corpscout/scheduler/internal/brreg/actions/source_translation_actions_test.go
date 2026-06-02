package actions

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSourceTranslationTaskCompletionFromResultExtractsTranslatedText(t *testing.T) {
	taskID := uuid.New()

	params, err := sourceTranslationCompletionParamsFromResult(TranslationRecordResult{
		RawRecordID:        taskID.String(),
		OrganizationNumber: "810202572",
		Status:             "succeeded",
		TranslatedPayload: map[string]any{
			"terms": []any{
				map[string]any{"translated_text": "Limited company"},
			},
		},
		Model:         "mock-fast",
		PromptVersion: "v1",
	}, taskID, 3)

	require.NoError(t, err)
	require.Equal(t, taskID, params.TaskID)
	require.Equal(t, "succeeded", params.Status)
	require.NotNil(t, params.TranslatedText)
	require.Equal(t, "Limited company", *params.TranslatedText)
	require.Equal(t, int32(3), params.MaxAttempts)
}
