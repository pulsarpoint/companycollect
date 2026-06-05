package workflow

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadFranceBulkRawRecordsPassesDatasetInputs(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadFranceBulkRawRecords)
	env.RegisterWorkflow(ProcessFranceSireneStockUniteLegale)
	env.RegisterWorkflow(ProcessFranceSireneStockEtablissement)

	var stageInput StageFranceBulkRawFilesActivityInput
	env.RegisterActivityWithOptions(func(input StageFranceBulkRawFilesActivityInput) (StageFranceBulkRawFilesActivityResult, error) {
		stageInput = input
		return StageFranceBulkRawFilesActivityResult{
			WorkflowRunID:              "workflow-run-id",
			SnapshotID:                 "snapshot-id",
			LegalUnitsSourceFileID:     "legal-source-file-id",
			EstablishmentsSourceFileID: "establishment-source-file-id",
			LegalUnitsPath:             "/var/lib/corpscout/worksets/france-sirene/workflow/stock_unite_legale.parquet",
			EstablishmentsPath:         "/var/lib/corpscout/worksets/france-sirene/workflow/stock_etablissement.parquet",
			LegalUnitsURL:              "https://example.test/StockUniteLegale.parquet",
			EstablishmentsURL:          "https://example.test/StockEtablissement.parquet",
			Limit:                      100,
			BatchSize:                  50,
			Metadata:                   []byte(`{"source":"sirene_bulk"}`),
		}, nil
	}, activity.RegisterOptions{Name: stageFranceBulkRawFilesActivity})

	var legalInput ProcessFranceSireneStockUniteLegaleActivityInput
	env.RegisterActivityWithOptions(func(input ProcessFranceSireneStockUniteLegaleActivityInput) (ProcessFranceSireneDatasetResult, error) {
		legalInput = input
		return ProcessFranceSireneDatasetResult{
			RowsSeen:              10,
			RowsWritten:           10,
			RowsInsertedNew:       9,
			RowsExistingUnchanged: 1,
		}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockUniteLegaleActivity})

	var establishmentInput ProcessFranceSireneStockEtablissementActivityInput
	env.RegisterActivityWithOptions(func(input ProcessFranceSireneStockEtablissementActivityInput) (ProcessFranceSireneDatasetResult, error) {
		establishmentInput = input
		return ProcessFranceSireneDatasetResult{
			RowsSeen:        20,
			RowsWritten:     20,
			RowsInsertedNew: 19,
			RowsNewVersions: 1,
		}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockEtablissementActivity})

	var finishInput FinishFranceBulkRawIngestActivityInput
	env.RegisterActivityWithOptions(func(input FinishFranceBulkRawIngestActivityInput) error {
		finishInput = input
		return nil
	}, activity.RegisterOptions{Name: finishFranceBulkRawIngestActivity})

	env.ExecuteWorkflow(LoadFranceBulkRawRecords, LoadFranceBulkRawRecordsInput{
		LegalUnitsURL:     "https://example.test/StockUniteLegale.parquet",
		EstablishmentsURL: "https://example.test/StockEtablissement.parquet",
		Limit:             100,
		BatchSize:         50,
		Trigger:           "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "https://example.test/StockUniteLegale.parquet", stageInput.LegalUnitsURL)
	require.Equal(t, "https://example.test/StockEtablissement.parquet", stageInput.EstablishmentsURL)
	require.EqualValues(t, 100, stageInput.Limit)
	require.EqualValues(t, 50, stageInput.BatchSize)
	require.Equal(t, "manual", stageInput.Trigger)

	require.Equal(t, "/var/lib/corpscout/worksets/france-sirene/workflow/stock_unite_legale.parquet", legalInput.SourcePath)
	require.Equal(t, "/var/lib/corpscout/worksets/france-sirene/workflow/stock_etablissement.parquet", establishmentInput.SourcePath)
	require.Equal(t, "legal-source-file-id", legalInput.SourceFileID)
	require.Equal(t, "establishment-source-file-id", establishmentInput.SourceFileID)
	require.Equal(t, "workflow-run-id", finishInput.WorkflowRunID)
	require.Equal(t, "snapshot-id", finishInput.SnapshotID)
	require.EqualValues(t, 30, finishInput.RowsSeen)
	require.EqualValues(t, 30, finishInput.RowsWritten)

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
	env.RegisterWorkflow(ProcessFranceSireneStockUniteLegale)
	env.RegisterWorkflow(ProcessFranceSireneStockEtablissement)
	attempts := 0
	env.RegisterActivityWithOptions(func(StageFranceBulkRawFilesActivityInput) (StageFranceBulkRawFilesActivityResult, error) {
		attempts++
		if attempts == 1 {
			return StageFranceBulkRawFilesActivityResult{}, stderrors.New("temporary parquet read failure")
		}
		return StageFranceBulkRawFilesActivityResult{
			WorkflowRunID:              "workflow-run-id",
			SnapshotID:                 "snapshot-id",
			LegalUnitsSourceFileID:     "legal-source-file-id",
			EstablishmentsSourceFileID: "establishment-source-file-id",
			LegalUnitsPath:             "/tmp/legal.parquet",
			EstablishmentsPath:         "/tmp/establishment.parquet",
			BatchSize:                  1000,
		}, nil
	}, activity.RegisterOptions{Name: stageFranceBulkRawFilesActivity})
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockUniteLegaleActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{RowsSeen: 1, RowsWritten: 1}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockUniteLegaleActivity})
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockEtablissementActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{RowsSeen: 1, RowsWritten: 1}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockEtablissementActivity})
	env.RegisterActivityWithOptions(func(FinishFranceBulkRawIngestActivityInput) error {
		return nil
	}, activity.RegisterOptions{Name: finishFranceBulkRawIngestActivity})

	env.ExecuteWorkflow(LoadFranceBulkRawRecords, LoadFranceBulkRawRecordsInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, attempts)
}

func TestLoadFranceBulkRawRecordsUsesLongHeartbeatTimeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadFranceBulkRawRecords)
	env.RegisterWorkflow(ProcessFranceSireneStockUniteLegale)
	env.RegisterWorkflow(ProcessFranceSireneStockEtablissement)
	env.RegisterActivityWithOptions(func(StageFranceBulkRawFilesActivityInput) (StageFranceBulkRawFilesActivityResult, error) {
		return StageFranceBulkRawFilesActivityResult{
			WorkflowRunID:              "workflow-run-id",
			SnapshotID:                 "snapshot-id",
			LegalUnitsSourceFileID:     "legal-source-file-id",
			EstablishmentsSourceFileID: "establishment-source-file-id",
			LegalUnitsPath:             "/tmp/legal.parquet",
			EstablishmentsPath:         "/tmp/establishment.parquet",
			BatchSize:                  1000,
		}, nil
	}, activity.RegisterOptions{Name: stageFranceBulkRawFilesActivity})
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockUniteLegaleActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockUniteLegaleActivity})
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockEtablissementActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockEtablissementActivity})
	env.RegisterActivityWithOptions(func(FinishFranceBulkRawIngestActivityInput) error {
		return nil
	}, activity.RegisterOptions{Name: finishFranceBulkRawIngestActivity})

	heartbeatTimeoutByActivity := map[string]time.Duration{}
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		heartbeatTimeoutByActivity[info.ActivityType.Name] = info.HeartbeatTimeout
	})

	env.ExecuteWorkflow(LoadFranceBulkRawRecords, LoadFranceBulkRawRecordsInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.GreaterOrEqual(t, heartbeatTimeoutByActivity[stageFranceBulkRawFilesActivity], 15*time.Minute)
}

func TestProcessFranceSireneStockWorkflowsUseLongHeartbeatTimeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(ProcessFranceSireneStockUniteLegale)
	env.RegisterWorkflow(ProcessFranceSireneStockEtablissement)
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockUniteLegaleActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockUniteLegaleActivity})
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockEtablissementActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockEtablissementActivity})

	heartbeatTimeoutByActivity := map[string]time.Duration{}
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		heartbeatTimeoutByActivity[info.ActivityType.Name] = info.HeartbeatTimeout
	})

	env.ExecuteWorkflow(ProcessFranceSireneStockUniteLegale, ProcessFranceSireneStockUniteLegaleInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.GreaterOrEqual(t, heartbeatTimeoutByActivity[processFranceSireneStockUniteLegaleActivity], 15*time.Minute)

	env = suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ProcessFranceSireneStockEtablissement)
	env.RegisterActivityWithOptions(func(ProcessFranceSireneStockEtablissementActivityInput) (ProcessFranceSireneDatasetResult, error) {
		return ProcessFranceSireneDatasetResult{}, nil
	}, activity.RegisterOptions{Name: processFranceSireneStockEtablissementActivity})
	heartbeatTimeoutByActivity = map[string]time.Duration{}
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		heartbeatTimeoutByActivity[info.ActivityType.Name] = info.HeartbeatTimeout
	})

	env.ExecuteWorkflow(ProcessFranceSireneStockEtablissement, ProcessFranceSireneStockEtablissementInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.GreaterOrEqual(t, heartbeatTimeoutByActivity[processFranceSireneStockEtablissementActivity], 15*time.Minute)
}

func TestLoadFranceBulkRawRecordsMarksRunFailedAfterActivityFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(LoadFranceBulkRawRecords)
	env.RegisterActivityWithOptions(func(StageFranceBulkRawFilesActivityInput) (StageFranceBulkRawFilesActivityResult, error) {
		return StageFranceBulkRawFilesActivityResult{}, stderrors.New("activity Heartbeat timeout")
	}, activity.RegisterOptions{Name: stageFranceBulkRawFilesActivity})

	var markerInput MarkFranceBulkRawIngestFailedActivityInput
	env.RegisterActivityWithOptions(func(input MarkFranceBulkRawIngestFailedActivityInput) error {
		markerInput = input
		return nil
	}, activity.RegisterOptions{Name: markFranceBulkRawIngestFailedActivity})

	env.ExecuteWorkflow(LoadFranceBulkRawRecords, LoadFranceBulkRawRecordsInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Equal(t, "default-test-workflow-id", markerInput.TemporalWorkflowID)
	require.Contains(t, markerInput.Error, "activity Heartbeat timeout")
}
