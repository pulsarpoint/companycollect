package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadSEBulkRawRecordsWorkflowRunsActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(ctx context.Context, input LoadSEBulkRawRecordsActivityInput) (LoadSEBulkRawRecordsActivityResult, error) {
			return LoadSEBulkRawRecordsActivityResult{}, nil
		},
		activity.RegisterOptions{Name: loadSEBulkRawRecordsActivity},
	)
	env.OnActivity(loadSEBulkRawRecordsActivity, mock.Anything, mock.MatchedBy(func(input LoadSEBulkRawRecordsActivityInput) bool {
		return input.TemporalWorkflowID != "" &&
			input.Trigger == "manual" &&
			input.Limit == 10 &&
			len(input.Datasets) == 1 &&
			input.Datasets[0].Dataset == "organisationer"
	})).Return(LoadSEBulkRawRecordsActivityResult{
		RowsSeen:    1,
		RowsWritten: 1,
		SourceFiles: []LoadSEBulkSourceFileResult{{
			Dataset:      "organisationer",
			SourceFileID: "source-file-1",
			RowsSeen:     1,
			RowsWritten:  1,
		}},
	}, nil).Once()

	env.ExecuteWorkflow(LoadSEBulkRawRecords, LoadSEBulkRawRecordsInput{
		Datasets: []HVDDatasetConfig{{
			Dataset: "organisationer",
			URL:     "https://example.test/se/organisationer.json",
			Format:  "json",
		}},
		Limit: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result LoadSEBulkRawRecordsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 1, result.RowsWritten)
}
