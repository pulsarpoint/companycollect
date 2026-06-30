package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestNorwayBRREGWorkflowLoadSignalRunsLoadActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(NorwayBRREGWorkflow)
	registerTestActivities(env)

	env.OnActivity(ActivityLoadNewInput, mock.Anything).Return(brreg.InitResult{RowsInserted: 7}, nil).Once()
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

	processInput := brreg.ProcessInput{BatchSize: 3, TimeoutSeconds: 30}
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(brreg.ProcessResult{TranslatedCount: 3}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(brreg.ProcessResult{TranslatedCount: 2}, nil).
		Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(brreg.ProcessResult{TranslatedCount: 0}, nil).
		Once()
	env.OnActivity(ActivityUploadOutput, mock.Anything).
		Return(brreg.UploadResult{RowsInserted: 5}, nil).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSourceAction, SourceActionSignal{Action: ActionRun})
	}, time.Millisecond)

	env.ExecuteWorkflow(NorwayBRREGWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func registerTestActivities(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(
		func(context.Context) (brreg.InitResult, error) {
			return brreg.InitResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityLoadNewInput},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, brreg.ProcessInput) (brreg.ProcessResult, error) {
			return brreg.ProcessResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityProcessOneBatch},
	)
	env.RegisterActivityWithOptions(
		func(context.Context) (brreg.UploadResult, error) {
			return brreg.UploadResult{}, nil
		},
		activity.RegisterOptions{Name: ActivityUploadOutput},
	)
}
