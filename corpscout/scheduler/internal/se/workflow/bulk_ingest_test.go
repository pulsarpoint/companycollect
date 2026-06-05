package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestLoadSEBulkRawRecordsWorkflowRunsSplitActivities(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(ctx context.Context, input VerifySEBulkSourceFilesActivityInput) (VerifySEBulkSourceFilesActivityResult, error) {
			return VerifySEBulkSourceFilesActivityResult{}, nil
		},
		activity.RegisterOptions{Name: verifySEBulkSourceFilesActivity},
	)
	env.RegisterActivityWithOptions(
		func(ctx context.Context, input CompareSEBulkSourceFilesActivityInput) (CompareSEBulkSourceFilesActivityResult, error) {
			return CompareSEBulkSourceFilesActivityResult{}, nil
		},
		activity.RegisterOptions{Name: compareSEBulkSourceFilesActivity},
	)
	env.RegisterActivityWithOptions(
		func(ctx context.Context, input DownloadSEBulkSourceFilesActivityInput) (DownloadSEBulkSourceFilesActivityResult, error) {
			return DownloadSEBulkSourceFilesActivityResult{}, nil
		},
		activity.RegisterOptions{Name: downloadSEBulkSourceFilesActivity},
	)
	env.RegisterActivityWithOptions(
		func(ctx context.Context, input ProcessSEBulkRawRecordsActivityInput) (ProcessSEBulkRawRecordsActivityResult, error) {
			return ProcessSEBulkRawRecordsActivityResult{}, nil
		},
		activity.RegisterOptions{Name: processSEBulkRawRecordsActivity},
	)

	env.OnActivity(verifySEBulkSourceFilesActivity, mock.Anything, mock.MatchedBy(func(input VerifySEBulkSourceFilesActivityInput) bool {
		return input.TemporalWorkflowID != "" &&
			input.Trigger == "manual" &&
			input.Limit == 10 &&
			len(input.Datasets) == 1 &&
			input.Datasets[0].Dataset == "organisationer"
	})).Return(VerifySEBulkSourceFilesActivityResult{
		WorkflowRunID: "workflow-run-1",
		SnapshotID:    "snapshot-1",
		Limit:         10,
		BatchSize:     1000,
		SourceFiles: []VerifiedSEBulkSourceFile{{
			Dataset:    "organisationer",
			SourceURL:  "https://example.test/se/organisationer.json",
			FileFormat: "json",
		}},
	}, nil).Once()
	env.OnActivity(compareSEBulkSourceFilesActivity, mock.Anything, mock.MatchedBy(func(input CompareSEBulkSourceFilesActivityInput) bool {
		return input.WorkflowRunID == "workflow-run-1" &&
			input.SnapshotID == "snapshot-1" &&
			len(input.SourceFiles) == 1 &&
			input.SourceFiles[0].Dataset == "organisationer"
	})).Return(CompareSEBulkSourceFilesActivityResult{
		WorkflowRunID: "workflow-run-1",
		SnapshotID:    "snapshot-1",
		Limit:         10,
		BatchSize:     1000,
		SourceFiles: []ComparedSEBulkSourceFile{{
			VerifiedSEBulkSourceFile: VerifiedSEBulkSourceFile{
				Dataset:    "organisationer",
				SourceURL:  "https://example.test/se/organisationer.json",
				FileFormat: "json",
			},
			NeedsDownload: true,
		}},
	}, nil).Once()
	env.OnActivity(downloadSEBulkSourceFilesActivity, mock.Anything, mock.MatchedBy(func(input DownloadSEBulkSourceFilesActivityInput) bool {
		return input.WorkflowRunID == "workflow-run-1" &&
			input.SnapshotID == "snapshot-1" &&
			len(input.SourceFiles) == 1 &&
			input.SourceFiles[0].NeedsDownload
	})).Return(DownloadSEBulkSourceFilesActivityResult{
		WorkflowRunID: "workflow-run-1",
		SnapshotID:    "snapshot-1",
		Limit:         10,
		BatchSize:     1000,
		SourceFiles: []DownloadedSEBulkSourceFile{{
			ComparedSEBulkSourceFile: ComparedSEBulkSourceFile{
				VerifiedSEBulkSourceFile: VerifiedSEBulkSourceFile{
					Dataset:    "organisationer",
					SourceURL:  "https://example.test/se/organisationer.json",
					FileFormat: "json",
				},
				NeedsDownload: true,
			},
			SourceFileID: "source-file-1",
			SourcePath:   "/tmp/organisationer.json",
			Status:       "downloaded",
		}},
	}, nil).Once()
	env.OnActivity(processSEBulkRawRecordsActivity, mock.Anything, mock.MatchedBy(func(input ProcessSEBulkRawRecordsActivityInput) bool {
		return input.WorkflowRunID == "workflow-run-1" &&
			input.SnapshotID == "snapshot-1" &&
			len(input.SourceFiles) == 1 &&
			input.SourceFiles[0].SourceFileID == "source-file-1"
	})).Return(ProcessSEBulkRawRecordsActivityResult{
		WorkflowRunID: "workflow-run-1",
		SnapshotID:    "snapshot-1",
		RowsSeen:      1,
		RowsWritten:   1,
		SourceFiles: []LoadSEBulkSourceFileResult{{
			Dataset:      "organisationer",
			SourceFileID: "source-file-1",
			SourceURL:    "https://example.test/se/organisationer.json",
			FileFormat:   "json",
			Status:       "parsed",
			RowsSeen:     1,
			RowsWritten:  1,
		}},
		Datasets: []LoadSEBulkDatasetLoadResult{{
			Dataset:     "organisationer",
			RowsSeen:    1,
			RowsWritten: 1,
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
