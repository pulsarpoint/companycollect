package workflow

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadFranceBulkRawRecordsPassesDatasetInputs(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadFranceBulkRawRecords)
	var activityInput LoadFranceBulkRawRecordsActivityInput
	env.RegisterActivityWithOptions(func(input LoadFranceBulkRawRecordsActivityInput) (LoadFranceBulkRawRecordsActivityResult, error) {
		activityInput = input
		return LoadFranceBulkRawRecordsActivityResult{
			LegalUnitsSeen:        10,
			LegalUnitsWritten:     10,
			EstablishmentsSeen:    20,
			EstablishmentsWritten: 20,
			RowsSeen:              30,
			RowsWritten:           30,
			RowsInsertedNew:       28,
			RowsExistingUnchanged: 1,
			RowsNewVersions:       1,
		}, nil
	}, activity.RegisterOptions{Name: loadFranceBulkRawRecordsActivity})

	env.ExecuteWorkflow(LoadFranceBulkRawRecords, LoadFranceBulkRawRecordsInput{
		LegalUnitsURL:     "https://example.test/StockUniteLegale.parquet",
		EstablishmentsURL: "https://example.test/StockEtablissement.parquet",
		Limit:             100,
		BatchSize:         50,
		Trigger:           "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "https://example.test/StockUniteLegale.parquet", activityInput.LegalUnitsURL)
	require.Equal(t, "https://example.test/StockEtablissement.parquet", activityInput.EstablishmentsURL)
	require.EqualValues(t, 100, activityInput.Limit)
	require.EqualValues(t, 50, activityInput.BatchSize)
	require.Equal(t, "manual", activityInput.Trigger)

	var result LoadFranceBulkRawRecordsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 30, result.RowsSeen)
	require.EqualValues(t, 30, result.RowsWritten)
	require.EqualValues(t, 10, result.LegalUnitsSeen)
	require.EqualValues(t, 20, result.EstablishmentsSeen)
}

func TestLoadFranceBulkRawRecordsRetriesTransientActivityFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadFranceBulkRawRecords)
	attempts := 0
	env.RegisterActivityWithOptions(func(LoadFranceBulkRawRecordsActivityInput) (LoadFranceBulkRawRecordsActivityResult, error) {
		attempts++
		if attempts == 1 {
			return LoadFranceBulkRawRecordsActivityResult{}, stderrors.New("temporary parquet read failure")
		}
		return LoadFranceBulkRawRecordsActivityResult{
			RowsSeen:    2,
			RowsWritten: 2,
		}, nil
	}, activity.RegisterOptions{Name: loadFranceBulkRawRecordsActivity})

	env.ExecuteWorkflow(LoadFranceBulkRawRecords, LoadFranceBulkRawRecordsInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, attempts)
}
