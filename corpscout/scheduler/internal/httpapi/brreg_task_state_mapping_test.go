package httpapi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregTaskStateAssetSummaryFromStateCopiesAllFields(t *testing.T) {
	state := brregTaskStateAssetFields{
		Asset:               "translation_results",
		RawRecordsCurrent:   1000,
		TaskNoState:         1,
		TaskPending:         2,
		TaskRunningActive:   3,
		TaskRunningStale:    4,
		TaskFailedRetryable: 5,
		TaskFailedTerminal:  6,
		TaskSucceeded:       7,
		TaskSkipped:         8,
		TaskEligibleNow:     9,
		ArtifactSucceeded:   10,
		ArtifactSkipped:     11,
		ArtifactFailed:      12,
		ArtifactMissing:     13,
	}

	require.Equal(t, brregTaskStateAssetSummary{
		Asset:               "translation_results",
		RawRecordsCurrent:   1000,
		TaskNoState:         1,
		TaskPending:         2,
		TaskRunningActive:   3,
		TaskRunningStale:    4,
		TaskFailedRetryable: 5,
		TaskFailedTerminal:  6,
		TaskSucceeded:       7,
		TaskSkipped:         8,
		TaskEligibleNow:     9,
		ArtifactSucceeded:   10,
		ArtifactSkipped:     11,
		ArtifactFailed:      12,
		ArtifactMissing:     13,
	}, brregTaskStateAssetSummaryFromState(state))
}
