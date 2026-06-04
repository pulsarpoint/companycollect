package workflow

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadCVRRawRecordsPassesScrollInput(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadCVRRawRecords)
	var activityInput LoadCVRRawRecordsActivityInput
	env.RegisterActivityWithOptions(func(input LoadCVRRawRecordsActivityInput) (LoadCVRRawRecordsActivityResult, error) {
		activityInput = input
		return LoadCVRRawRecordsActivityResult{
			RowsSeen:              100,
			RowsWritten:           100,
			RowsInsertedNew:       99,
			RowsExistingUnchanged: 1,
		}, nil
	}, activity.RegisterOptions{Name: loadCVRRawRecordsActivity})

	env.ExecuteWorkflow(LoadCVRRawRecords, LoadCVRRawRecordsInput{
		SourceURL: "https://example.test/cvr-permanent/virksomhed/_search",
		ScrollURL: "https://example.test/_search/scroll",
		Scroll:    "100",
		Limit:     100,
		PageSize:  25,
		BatchSize: 50,
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 100, activityInput.Limit)
	require.EqualValues(t, 25, activityInput.PageSize)
	require.EqualValues(t, 50, activityInput.BatchSize)
	require.Equal(t, "100", activityInput.Scroll)
	require.Equal(t, "manual", activityInput.Trigger)

	var result LoadCVRRawRecordsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 100, result.RowsSeen)
	require.EqualValues(t, 100, result.RowsWritten)
}

func TestLoadCVRRawRecordsRetriesTransientActivityFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadCVRRawRecords)
	attempts := 0
	env.RegisterActivityWithOptions(func(LoadCVRRawRecordsActivityInput) (LoadCVRRawRecordsActivityResult, error) {
		attempts++
		if attempts == 1 {
			return LoadCVRRawRecordsActivityResult{}, stderrors.New("temporary EOF")
		}
		return LoadCVRRawRecordsActivityResult{
			RowsSeen:    2,
			RowsWritten: 2,
		}, nil
	}, activity.RegisterOptions{Name: loadCVRRawRecordsActivity})

	env.ExecuteWorkflow(LoadCVRRawRecords, LoadCVRRawRecordsInput{
		Limit:   2,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, attempts)
}
