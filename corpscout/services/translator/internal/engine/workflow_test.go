package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestTemporalIdentityConstants(t *testing.T) {
	if ProcessWorkflowID != "translator/process" {
		t.Fatalf("ProcessWorkflowID = %q", ProcessWorkflowID)
	}
	if ProcessTaskQueue != "translator-process" {
		t.Fatalf("ProcessTaskQueue = %q", ProcessTaskQueue)
	}
	if ActivityProcessOneBatch != "translator.ProcessOneBatch" {
		t.Fatalf("ActivityProcessOneBatch = %q", ActivityProcessOneBatch)
	}
	if ActivityFlushOutput != "translator.FlushOutput" {
		t.Fatalf("ActivityFlushOutput = %q", ActivityFlushOutput)
	}
	if SignalNewItems != "new-items" {
		t.Fatalf("SignalNewItems = %q", SignalNewItems)
	}
}

func newWorkflowTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	env.RegisterActivityWithOptions(
		func(context.Context, ProcessInput) (ProcessResult, error) { return ProcessResult{}, nil },
		activity.RegisterOptions{Name: ActivityProcessOneBatch},
	)
	env.RegisterActivityWithOptions(
		func(context.Context) (UploadResult, error) { return UploadResult{}, nil },
		activity.RegisterOptions{Name: ActivityFlushOutput},
	)
	return env
}

func TestTranslationWorkflowProcessesUntilEmptyThenFlushes(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 2, OutputCount: 3}, nil).Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 2, PendingCount: 0, OutputCount: 5}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 5, RowsInserted: 5}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowCompletesWithoutFlushWhenQueueEmpty(t *testing.T) {
	env := newWorkflowTestEnv(t)
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, mock.Anything).
		Return(ProcessResult{TranslatedCount: 0, PendingCount: 0, OutputCount: 0}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowFlushesEveryNBatches(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	// 3 translated batches with FlushEveryBatches=2: flush after batch 2,
	// then queue empties on batch 3 → final flush.
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 9, OutputCount: 3}, nil).Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 6, OutputCount: 6}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 6, RowsInserted: 6}, nil).Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 0, OutputCount: 3}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 3, RowsInserted: 3}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30, FlushEveryBatches: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowContinuesAsNewAfterBatchLimit(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 9, OutputCount: 3}, nil).Twice()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{
		BatchSize: 3, TimeoutSeconds: 30, BatchesPerRun: 2, FlushEveryBatches: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.True(t, workflow.IsContinueAsNewError(err), "expected continue-as-new, got %v", err)
	env.AssertExpectations(t)
}

func TestTranslationWorkflowSignalTriggersAnotherRoundAfterEmpty(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 1, PendingCount: 0, OutputCount: 1}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 1, RowsInserted: 1}, nil).Once()
	// Signal delivered before the first batch completes → after flush the
	// workflow loops once more instead of completing.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalNewItems, nil)
	}, 0)
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 0, PendingCount: 0, OutputCount: 0}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
