package nacetaxonomy

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestSyncNACETaxonomyWorkflowCallsActivityWithDefaults(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(SyncNACETaxonomy)
	env.RegisterActivityWithOptions(func(input SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
		require.NotEmpty(t, input.TemporalWorkflowID)
		require.Equal(t, DefaultRevision, input.Revision)
		require.Equal(t, "manual", input.Trigger)
		require.Equal(t, "https://example.test/nace.rdf", input.SourceURL)
		require.True(t, input.ForceReprocess)
		return SyncNACETaxonomyActivityResult{
			Status:          SyncStatusSucceeded,
			ImportRunID:     "run-1",
			SourceFileID:    "source-1",
			ContentSHA256:   "hash-1",
			RecordsSeen:     2,
			RecordsImported: 2,
		}, nil
	}, activity.RegisterOptions{Name: syncNACETaxonomyActivity})

	env.ExecuteWorkflow(SyncNACETaxonomy, SyncNACETaxonomyInput{
		SourceURL:      "https://example.test/nace.rdf",
		ForceReprocess: true,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncNACETaxonomyResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, SyncStatusSucceeded, result.Status)
	require.Equal(t, int32(2), result.RecordsImported)
}

func TestSyncNACETaxonomyWorkflowReturnsSkippedResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(SyncNACETaxonomy)
	env.RegisterActivityWithOptions(func(input SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
		require.Equal(t, "2.0", input.Revision)
		require.Equal(t, "scheduled", input.Trigger)
		return SyncNACETaxonomyActivityResult{
			Status:        SyncStatusSkipped,
			ImportRunID:   "run-2",
			SourceFileID:  "source-2",
			ContentSHA256: "hash-2",
			Message:       "source file hash already processed",
		}, nil
	}, activity.RegisterOptions{Name: syncNACETaxonomyActivity})

	env.ExecuteWorkflow(SyncNACETaxonomy, SyncNACETaxonomyInput{
		Revision:  "2.0",
		SourceURL: "https://example.test/nace.rdf",
		Trigger:   "scheduled",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncNACETaxonomyResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, SyncStatusSkipped, result.Status)
	require.Equal(t, "source file hash already processed", result.Message)
}
