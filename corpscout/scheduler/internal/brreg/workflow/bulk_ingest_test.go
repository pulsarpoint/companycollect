package workflow

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadBrregBulkRawRecordsAllowsAllRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadBrregBulkRawRecords)
	var activityInput LoadBrregBulkRawRecordsActivityInput
	env.RegisterActivityWithOptions(func(input LoadBrregBulkRawRecordsActivityInput) (LoadBrregBulkRawRecordsActivityResult, error) {
		activityInput = input
		return LoadBrregBulkRawRecordsActivityResult{
			RowsSeen:              1172125,
			RowsWritten:           1172125,
			RowsInsertedNew:       1172125,
			RowsExistingUnchanged: 0,
			RowsNewVersions:       0,
		}, nil
	}, activity.RegisterOptions{Name: loadBrregBulkRawRecordsActivity})

	env.ExecuteWorkflow(LoadBrregBulkRawRecords, LoadBrregBulkRawRecordsInput{
		Limit:     0,
		SourceURL: "https://example.test/enheter_alle.json.gz",
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 0, activityInput.Limit)
	require.Equal(t, "https://example.test/enheter_alle.json.gz", activityInput.SourceURL)
	require.Equal(t, "manual", activityInput.Trigger)

	var result LoadBrregBulkRawRecordsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 1172125, result.RowsSeen)
	require.EqualValues(t, 1172125, result.RowsWritten)
}

func TestLoadBrregBulkRawRecordsRetriesTransientActivityFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadBrregBulkRawRecords)
	attempts := 0
	env.RegisterActivityWithOptions(func(LoadBrregBulkRawRecordsActivityInput) (LoadBrregBulkRawRecordsActivityResult, error) {
		attempts++
		if attempts == 1 {
			return LoadBrregBulkRawRecordsActivityResult{}, stderrors.New("temporary EOF")
		}
		return LoadBrregBulkRawRecordsActivityResult{
			RowsSeen:    2,
			RowsWritten: 2,
		}, nil
	}, activity.RegisterOptions{Name: loadBrregBulkRawRecordsActivity})

	env.ExecuteWorkflow(LoadBrregBulkRawRecords, LoadBrregBulkRawRecordsInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, attempts)
}
