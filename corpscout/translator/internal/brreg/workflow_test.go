package brreg

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

func TestNorwayBRREGWorkflowLoadSignalRunsLoadActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(NorwayBRREGWorkflow)
	registerTestActivities(env)

	env.OnActivity(ActivityLoadNewInput, mock.Anything).Return(InitResult{RowsInserted: 7}, nil).Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionLoadQueue})
	}, time.Millisecond)

	env.ExecuteWorkflow(NorwayBRREGWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestNorwayBRREGWorkflowProcessSignalRunsBatchesUntilEmptyThenUploads(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(NorwayBRREGWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 2, OutputCount: 3}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 2, PendingCount: 0, OutputCount: 5}, nil).
		Once()
	env.OnActivity(ActivityUploadOutput, mock.Anything).
		Return(UploadResult{RowsInserted: 5}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(NorwayBRREGWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestNorwayBRREGWorkflowLoadAndRunSignalLoadsBeforeProcessing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(NorwayBRREGWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityLoadNewInput, mock.Anything).
		Return(InitResult{RowsInserted: 7}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 0, OutputCount: 3}, nil).
		Once()
	env.OnActivity(ActivityUploadOutput, mock.Anything).
		Return(UploadResult{RowsInserted: 3}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionLoadAndRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(NorwayBRREGWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestNorwayBRREGWorkflowContinuesAsNewAfterBatchLimit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(NorwayBRREGWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 9, OutputCount: 3}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 6, OutputCount: 6}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(NorwayBRREGWorkflow, WorkflowInput{
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

func TestNorwayBRREGWorkflowContinuedRunProcessesWithoutSignal(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(NorwayBRREGWorkflow)
	registerTestActivities(env)

	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 0, PendingCount: 0, OutputCount: 0}, nil).
		Once()

	env.ExecuteWorkflow(NorwayBRREGWorkflow, WorkflowInput{
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
		func(context.Context) (InitResult, error) {
			return InitResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityLoadNewInput},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, ProcessInput) (ProcessResult, error) {
			return ProcessResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityProcessOneBatch},
	)
	env.RegisterActivityWithOptions(
		func(context.Context) (UploadResult, error) {
			return UploadResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityUploadOutput},
	)
}
