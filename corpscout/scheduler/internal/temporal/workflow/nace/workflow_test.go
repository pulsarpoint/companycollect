package nace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	domainnace "github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func TestSyncNACETaxonomyWorkflowCallsActivityWithDefaults(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncNACETaxonomy, workflow.RegisterOptions{Name: SyncWorkflowName})
	env.RegisterWorkflowWithOptions(SyncNACEToClickHouse, workflow.RegisterOptions{Name: SyncToClickHouseWorkflowName})
	env.RegisterActivityWithOptions(func(_ context.Context, input SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
		require.NotEmpty(t, input.TemporalWorkflowID)
		require.Equal(t, domainnace.DefaultRevision, input.Revision)
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
	}, activity.RegisterOptions{Name: SyncNACETaxonomyActivityName})
	env.OnWorkflow(SyncToClickHouseWorkflowName, mock.Anything, SyncNACEToClickHouseInput{
		Trigger: "nace_taxonomy_sync",
	}).Return(SyncNACEToClickHouseResult{Status: SyncStatusSucceeded, CodesSynced: 2}, nil)

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
	env.AssertExpectations(t)
}

func TestSyncNACETaxonomyWorkflowReturnsSkippedResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncNACETaxonomy, workflow.RegisterOptions{Name: SyncWorkflowName})
	env.RegisterWorkflowWithOptions(SyncNACEToClickHouse, workflow.RegisterOptions{Name: SyncToClickHouseWorkflowName})
	env.RegisterActivityWithOptions(func(_ context.Context, input SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
		require.Equal(t, "2.0", input.Revision)
		require.Equal(t, "scheduled", input.Trigger)
		return SyncNACETaxonomyActivityResult{
			Status:        SyncStatusSkipped,
			ImportRunID:   "run-2",
			SourceFileID:  "source-2",
			ContentSHA256: "hash-2",
			Message:       "source file hash already processed",
		}, nil
	}, activity.RegisterOptions{Name: SyncNACETaxonomyActivityName})
	env.OnWorkflow(SyncToClickHouseWorkflowName, mock.Anything, SyncNACEToClickHouseInput{
		Trigger: "nace_taxonomy_sync",
	}).Return(SyncNACEToClickHouseResult{Status: SyncStatusSucceeded}, nil)

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
	env.AssertExpectations(t)
}

func TestSyncNACEToClickHouseWorkflowRunsActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncNACEToClickHouse, workflow.RegisterOptions{Name: SyncToClickHouseWorkflowName})
	env.RegisterActivityWithOptions(func(_ context.Context, input SyncNACEToClickHouseActivityInput) (SyncNACEToClickHouseActivityResult, error) {
		require.NotEmpty(t, input.TemporalWorkflowID)
		require.Equal(t, "manual", input.Trigger)
		return SyncNACEToClickHouseActivityResult{Status: SyncStatusSucceeded, CodesSynced: 4}, nil
	}, activity.RegisterOptions{Name: SyncNACEToClickHouseActivityName})

	env.ExecuteWorkflow(SyncNACEToClickHouse, SyncNACEToClickHouseInput{Trigger: "manual"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncNACEToClickHouseResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, SyncStatusSucceeded, result.Status)
	require.Equal(t, 4, result.CodesSynced)
}

func TestSyncNACETaxonomyStartsClickHouseSyncAfterSuccessfulImport(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncNACETaxonomy, workflow.RegisterOptions{Name: SyncWorkflowName})
	env.RegisterWorkflowWithOptions(SyncNACEToClickHouse, workflow.RegisterOptions{Name: SyncToClickHouseWorkflowName})
	env.RegisterActivityWithOptions(func(_ context.Context, _ SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
		return SyncNACETaxonomyActivityResult{
			Status:          SyncStatusSucceeded,
			ImportRunID:     "import-run-1",
			SourceFileID:    "source-file-1",
			ContentSHA256:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			RecordsSeen:     4,
			RecordsImported: 4,
			Message:         "nace taxonomy imported",
		}, nil
	}, activity.RegisterOptions{Name: SyncNACETaxonomyActivityName})
	env.OnWorkflow(SyncToClickHouseWorkflowName, mock.Anything, SyncNACEToClickHouseInput{
		Trigger: "nace_taxonomy_sync",
	}).Return(SyncNACEToClickHouseResult{
		Status:      SyncStatusSucceeded,
		CodesSynced: 4,
	}, nil)

	env.ExecuteWorkflow(SyncNACETaxonomy, SyncNACETaxonomyInput{
		Revision:  "2.1",
		SourceURL: "https://example.test/nace.rdf",
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
