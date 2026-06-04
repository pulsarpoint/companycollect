package workflow

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadAriregisterBulkRawRecordsAllowsAllRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadAriregisterBulkRawRecords)
	var activityInput LoadAriregisterBulkRawRecordsActivityInput
	env.RegisterActivityWithOptions(func(input LoadAriregisterBulkRawRecordsActivityInput) (LoadAriregisterBulkRawRecordsActivityResult, error) {
		activityInput = input
		return LoadAriregisterBulkRawRecordsActivityResult{
			RowsSeen:              42,
			RowsWritten:           42,
			RowsInsertedNew:       40,
			RowsExistingUnchanged: 1,
			RowsNewVersions:       1,
			SnapshotID:            "snapshot-id",
			SourceFileID:          "file-id",
			FileName:              "ettevotjad.csv",
		}, nil
	}, activity.RegisterOptions{Name: loadAriregisterBulkRawRecordsActivity})

	env.ExecuteWorkflow(LoadAriregisterBulkRawRecords, LoadAriregisterBulkRawRecordsInput{
		Limit:     0,
		SourceURL: "https://example.test/ettevotjad.csv.zip",
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 0, activityInput.Limit)
	require.Equal(t, "https://example.test/ettevotjad.csv.zip", activityInput.SourceURL)
	require.Equal(t, "manual", activityInput.Trigger)

	var result LoadAriregisterBulkRawRecordsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 42, result.RowsSeen)
	require.EqualValues(t, 42, result.RowsWritten)
	require.Equal(t, "snapshot-id", result.SnapshotID)
	require.Equal(t, "ettevotjad.csv", result.FileName)
}

func TestLoadAriregisterBulkRawRecordsRetriesTransientActivityFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadAriregisterBulkRawRecords)
	attempts := 0
	env.RegisterActivityWithOptions(func(LoadAriregisterBulkRawRecordsActivityInput) (LoadAriregisterBulkRawRecordsActivityResult, error) {
		attempts++
		if attempts == 1 {
			return LoadAriregisterBulkRawRecordsActivityResult{}, stderrors.New("temporary EOF")
		}
		return LoadAriregisterBulkRawRecordsActivityResult{
			RowsSeen:    2,
			RowsWritten: 2,
		}, nil
	}, activity.RegisterOptions{Name: loadAriregisterBulkRawRecordsActivity})

	env.ExecuteWorkflow(LoadAriregisterBulkRawRecords, LoadAriregisterBulkRawRecordsInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, attempts)
}
