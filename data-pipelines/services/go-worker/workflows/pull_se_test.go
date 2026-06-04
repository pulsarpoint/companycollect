package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

type PullSESuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullSESuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, input contracts.DownloadSourceFilesInput) (contracts.DownloadSourceFilesResult, error) {
			return contracts.DownloadSourceFilesResult{}, nil
		},
		activity.RegisterOptions{Name: "download_se_hvd_dataset"},
	)
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *PullSESuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullSESuite(t *testing.T) {
	suite.Run(t, new(PullSESuite))
}

func (s *PullSESuite) Test_Full_DownloadsImportsAndMarksComplete() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "se",
		SnapshotID: "se-2026-06-04",
		Files: []contracts.DownloadedSourceFile{
			{Source: "se", Dataset: "organisationer", FilePath: "/tmp/se-organisationer.json", SnapshotID: "se-2026-06-04", SHA256: "abc", Format: "json"},
			{Source: "se", Dataset: "arsredovisningar", FilePath: "/tmp/se-arsredovisningar.json", SnapshotID: "se-2026-06-04", SHA256: "def", Format: "json"},
		},
	}

	s.env.OnActivity("download_se_hvd_dataset", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "se" &&
			input.Mode == "full" &&
			input.OutputDir == "/tmp/se-out"
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportSEHVDBulk, mock.Anything, mock.MatchedBy(func(params contracts.ImportSEHVDBulkParams) bool {
		return params.RunID == "run-se" &&
			params.CorpscoutRunID == "exec-se" &&
			len(params.Files) == 2
	})).Return(42, nil).Once()

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.RunID == "run-se" &&
			params.CorpscoutRunID == "exec-se" &&
			params.Source == "se" &&
			params.Country == "SE" &&
			params.Result.RecordsWritten == 42 &&
			params.Result.PagesFetched == 2 &&
			params.FinalCursor == "full:se-2026-06-04"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullSE, contracts.PullSEInput{
		CorpscoutRunID: "exec-se",
		RunID:          "run-se",
		Mode:           "full",
		OutputDir:      "/tmp/se-out",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(42, result.RecordsWritten)
	s.Equal(2, result.PagesFetched)
}

func (s *PullSESuite) Test_InvalidModeFailsBeforeActivities() {
	s.env.ExecuteWorkflow(workflows.PullSE, contracts.PullSEInput{
		RunID: "run-se-invalid",
		Mode:  "delta",
	})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Error(err)
	s.Contains(err.Error(), "unsupported se mode")
}
