package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const workflowTestSource = "norway_brreg"

// TestTemporalIdentityHelpersPreserveNorwayIdentity pins the Norway BRREG
// Temporal identity: the workflow ID and task queue must never change, or a
// deployed worker will silently stop serving the existing workflow.
func TestTemporalIdentityHelpersPreserveNorwayIdentity(t *testing.T) {
	if got := WorkflowID("norway_brreg"); got != "translator/norway_brreg" {
		t.Fatalf("WorkflowID = %q, want translator/norway_brreg", got)
	}
	if got := TaskQueue("norway_brreg"); got != "translator-norway-brreg" {
		t.Fatalf("TaskQueue = %q, want translator-norway-brreg", got)
	}
	if got := ActivityLoadNewInput("norway_brreg"); got != "norway_brreg.LoadNewInput" {
		t.Fatalf("ActivityLoadNewInput = %q, want norway_brreg.LoadNewInput", got)
	}
	if got := ActivityProcessOneBatch("norway_brreg"); got != "norway_brreg.ProcessOneBatch" {
		t.Fatalf("ActivityProcessOneBatch = %q, want norway_brreg.ProcessOneBatch", got)
	}
	if got := ActivityUploadOutput("norway_brreg"); got != "norway_brreg.UploadOutput" {
		t.Fatalf("ActivityUploadOutput = %q, want norway_brreg.UploadOutput", got)
	}
}

func TestTranslationWorkflowRequiresSource(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestTranslationWorkflowLoadSignalRunsLoadActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	registerTestActivities(env)

	env.OnActivity(ActivityLoadNewInput(workflowTestSource), mock.Anything).Return(LoadResult{RowsInserted: 7}, nil).Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionLoadQueue})
	}, time.Millisecond)

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{Source: workflowTestSource, BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowProcessSignalRunsBatchesUntilEmptyThenUploads(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch(workflowTestSource), mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 2, OutputCount: 3}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch(workflowTestSource), mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 2, PendingCount: 0, OutputCount: 5}, nil).
		Once()
	env.OnActivity(ActivityUploadOutput(workflowTestSource), mock.Anything).
		Return(UploadResult{RowsInserted: 5}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{Source: workflowTestSource, BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowLoadAndRunSignalLoadsBeforeProcessing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityLoadNewInput(workflowTestSource), mock.Anything).
		Return(LoadResult{RowsInserted: 7}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch(workflowTestSource), mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 0, OutputCount: 3}, nil).
		Once()
	env.OnActivity(ActivityUploadOutput(workflowTestSource), mock.Anything).
		Return(UploadResult{RowsInserted: 3}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionLoadAndRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{Source: workflowTestSource, BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowContinuesAsNewAfterBatchLimit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch(workflowTestSource), mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 9, OutputCount: 3}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch(workflowTestSource), mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 6, OutputCount: 6}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{
		Source:         workflowTestSource,
		BatchSize:      3,
		TimeoutSeconds: 30,
		BatchesPerRun:  2,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.True(t, workflow.IsContinueAsNewError(err), "expected continue-as-new error, got %v", err)
	env.AssertExpectations(t)
}

func TestTranslationWorkflowContinuedRunProcessesWithoutSignal(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch(workflowTestSource), mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 0, PendingCount: 0, OutputCount: 0}, nil).
		Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{
		Source:         workflowTestSource,
		BatchSize:      3,
		TimeoutSeconds: 30,
		BatchesPerRun:  2,
		ResumeAction:   ActionRun,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func registerTestActivities(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(
		func(context.Context) (LoadResult, error) {
			return LoadResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityLoadNewInput(workflowTestSource)},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, ProcessInput) (ProcessResult, error) {
			return ProcessResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityProcessOneBatch(workflowTestSource)},
	)
	env.RegisterActivityWithOptions(
		func(context.Context) (UploadResult, error) {
			return UploadResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityUploadOutput(workflowTestSource)},
	)
}
