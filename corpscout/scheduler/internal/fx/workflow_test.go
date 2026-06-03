package fx

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestSyncExchangeRatesWorkflowCallsActivityWithDefaults(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(SyncExchangeRates)
	env.RegisterActivityWithOptions(func(input SyncExchangeRatesActivityInput) (SyncExchangeRatesActivityResult, error) {
		require.NotEmpty(t, input.TemporalWorkflowID)
		require.Equal(t, DefaultProvider, input.Provider)
		require.Equal(t, DefaultDailySourceURL, input.SourceURL)
		require.Equal(t, "manual", input.Trigger)
		require.True(t, input.ForceReprocess)
		return SyncExchangeRatesActivityResult{
			Status:             SyncStatusSucceeded,
			SyncRunID:          "run-1",
			SourceFileID:       "source-1",
			SheetID:            "sheet-1",
			ContentSHA256:      "hash-1",
			RateDate:           "2026-06-03",
			CurrenciesSeen:     4,
			CurrenciesImported: 4,
		}, nil
	}, activity.RegisterOptions{Name: syncExchangeRatesActivity})

	env.ExecuteWorkflow(SyncExchangeRates, SyncExchangeRatesInput{ForceReprocess: true})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncExchangeRatesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, SyncStatusSucceeded, result.Status)
	require.Equal(t, int32(4), result.CurrenciesImported)
}

func TestSyncExchangeRatesWorkflowReturnsSkippedResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(SyncExchangeRates)
	env.RegisterActivityWithOptions(func(input SyncExchangeRatesActivityInput) (SyncExchangeRatesActivityResult, error) {
		require.Equal(t, "scheduled", input.Trigger)
		require.Equal(t, "https://example.test/ecb.xml", input.SourceURL)
		return SyncExchangeRatesActivityResult{
			Status:        SyncStatusSkipped,
			SyncRunID:     "run-2",
			SourceFileID:  "source-2",
			ContentSHA256: "hash-2",
			Message:       "source file hash already processed",
		}, nil
	}, activity.RegisterOptions{Name: syncExchangeRatesActivity})

	env.ExecuteWorkflow(SyncExchangeRates, SyncExchangeRatesInput{
		SourceURL: "https://example.test/ecb.xml",
		Trigger:   "scheduled",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncExchangeRatesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, SyncStatusSkipped, result.Status)
	require.Equal(t, "source file hash already processed", result.Message)
}
