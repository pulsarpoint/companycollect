package companysources

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSyncSourceToClickHouseRunsDownloadThenImport(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(DownloadSource, workflow.RegisterOptions{Name: DownloadSourceWorkflowName})
	env.RegisterWorkflowWithOptions(ImportSourceToClickHouse, workflow.RegisterOptions{Name: ImportSourceToClickHouseWorkflowName})
	env.RegisterWorkflowWithOptions(SyncSourceToClickHouse, workflow.RegisterOptions{Name: SyncSourceToClickHouseWorkflowName})

	env.OnWorkflow(DownloadSourceWorkflowName, mock.Anything, SyncSourceDownloadInput{
		ActionRunID: "download-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	}).Return(DownloadSourceResult{
		ActionRunID: "download-run-1",
		Files: []DownloadedSourceFileSummary{
			{FileKey: "source", FileRunID: "file-run-1", Path: "/tmp/source.ndjson", RecordsWritten: 2},
		},
	}, nil)
	env.OnWorkflow(ImportSourceToClickHouseWorkflowName, mock.Anything, ImportSourceToClickHouseInput{
		ActionRunID:         "import-run-1",
		SourceName:          "finland_prhytj",
		Trigger:             "manual",
		DownloadActionRunID: "download-run-1",
		FileRunIDs:          []string{"file-run-1"},
		BatchSize:           1000,
	}).Return(ImportSourceToClickHouseResult{
		ActionRunID:  "import-run-1",
		ImportedRows: 2,
	}, nil)

	env.ExecuteWorkflow(SyncSourceToClickHouseWorkflowName, SyncSourceToClickHouseInput{
		DownloadActionRunID: "download-run-1",
		ImportActionRunID:   "import-run-1",
		SourceName:          "finland_prhytj",
		Trigger:             "manual",
		BatchSize:           1000,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncSourceToClickHouseResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "download-run-1", result.Download.ActionRunID)
	require.Equal(t, "import-run-1", result.Import.ActionRunID)
}

func TestDownloadSourceRunsPreparedFileWorkflows(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(DownloadSource, workflow.RegisterOptions{Name: DownloadSourceWorkflowName})
	env.RegisterWorkflowWithOptions(DownloadSourceFile, workflow.RegisterOptions{Name: DownloadSourceFileWorkflowName})
	env.RegisterActivityWithOptions(func(input SyncSourceDownloadInput) (PrepareSourceDownloadResult, error) {
		require.Equal(t, SyncSourceDownloadInput{
			ActionRunID: "action-run-1",
			SourceName:  "finland_prhytj",
			Trigger:     "manual",
		}, input)
		return PrepareSourceDownloadResult{
			ActionRunID: "action-run-1",
			Files: []DownloadSourceFileInput{
				{
					FileRunID:         "file-run-1",
					SourceName:        "finland_prhytj",
					FileKey:           "source",
					Trigger:           "manual",
					ParentActionRunID: "action-run-1",
				},
			},
		}, nil
	}, activity.RegisterOptions{Name: PrepareSourceDownloadActivityName})
	env.RegisterActivityWithOptions(func(input FinishSourceDownloadInput) error {
		require.Equal(t, FinishSourceDownloadInput{
			ActionRunID: "action-run-1",
			Status:      StatusSucceeded,
			Files: []DownloadedSourceFileSummary{
				{FileKey: "source", FileRunID: "file-run-1", Path: "/tmp/source.ndjson", RecordsWritten: 2},
			},
		}, input)
		return nil
	}, activity.RegisterOptions{Name: FinishSourceDownloadActivityName})

	env.OnWorkflow(DownloadSourceFileWorkflowName, mock.Anything, DownloadSourceFileInput{
		FileRunID:         "file-run-1",
		SourceName:        "finland_prhytj",
		FileKey:           "source",
		Trigger:           "manual",
		ParentActionRunID: "action-run-1",
	}).Return(DownloadSourceFileResult{
		FileRunID:      "file-run-1",
		SourceName:     "finland_prhytj",
		FileKey:        "source",
		Path:           "/tmp/source.ndjson",
		RecordsWritten: 2,
	}, nil)

	env.ExecuteWorkflow(DownloadSourceWorkflowName, SyncSourceDownloadInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DownloadSourceResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "action-run-1", result.ActionRunID)
	require.Equal(t, []DownloadedSourceFileSummary{
		{FileKey: "source", FileRunID: "file-run-1", Path: "/tmp/source.ndjson", RecordsWritten: 2},
	}, result.Files)
}

func TestDownloadSourcePropagatesFinancialXBRLWindowInput(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(DownloadSourceFile, workflow.RegisterOptions{Name: DownloadSourceFileWorkflowName})

	prepared := PrepareSourceDownloadResult{
		ActionRunID: "action-1",
		Files: []DownloadSourceFileInput{{
			FileRunID:         "file-1",
			SourceName:        "finland_prh_xbrl",
			FileKey:           "statements_manifest",
			Trigger:           "manual",
			ParentActionRunID: "action-1",
		}},
	}

	env.RegisterActivityWithOptions(func(input SyncSourceDownloadInput) (PrepareSourceDownloadResult, error) {
		require.Equal(t, SyncSourceDownloadInput{
			ActionRunID:         "action-1",
			SourceName:          "finland_prh_xbrl",
			Trigger:             "manual",
			RegisteredDateStart: "2026-06-01",
			RegisteredDateEnd:   "2026-06-03",
			MaxStatements:       50,
			RetryFailed:         true,
		}, input)
		return prepared, nil
	}, activity.RegisterOptions{Name: PrepareSourceDownloadActivityName})
	env.RegisterActivityWithOptions(func(input FinishSourceDownloadInput) error {
		require.Equal(t, FinishSourceDownloadInput{
			ActionRunID: "action-1",
			Status:      StatusSucceeded,
			Files: []DownloadedSourceFileSummary{
				{FileKey: "statements_manifest", FileRunID: "file-1", Path: "/tmp/statements.ndjson", RecordsWritten: 1},
			},
		}, input)
		return nil
	}, activity.RegisterOptions{Name: FinishSourceDownloadActivityName})
	env.OnWorkflow(DownloadSourceFileWorkflowName, mock.Anything, DownloadSourceFileInput{
		FileRunID:           "file-1",
		SourceName:          "finland_prh_xbrl",
		FileKey:             "statements_manifest",
		Trigger:             "manual",
		ParentActionRunID:   "action-1",
		RegisteredDateStart: "2026-06-01",
		RegisteredDateEnd:   "2026-06-03",
		MaxStatements:       50,
		RetryFailed:         true,
	}).Return(DownloadSourceFileResult{
		FileRunID:          "file-1",
		SourceName:         "finland_prh_xbrl",
		FileKey:            "statements_manifest",
		Path:               "/tmp/statements.ndjson",
		ContentSHA256:      "abc",
		ContentLengthBytes: 12,
		RecordsWritten:     1,
	}, nil)

	env.ExecuteWorkflow(DownloadSource, SyncSourceDownloadInput{
		ActionRunID:         "action-1",
		SourceName:          "finland_prh_xbrl",
		Trigger:             "manual",
		RegisteredDateStart: "2026-06-01",
		RegisteredDateEnd:   "2026-06-03",
		MaxStatements:       50,
		RetryFailed:         true,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestSourceWorkflowIDHelpersUseRunIDs(t *testing.T) {
	require.Equal(t, "company-source-action-run-action-run-1", ActionRunWorkflowID("action-run-1"))
	require.Equal(t, "company-source-file-run-file-run-1", FileRunWorkflowID("file-run-1"))
}

func TestRefreshSourceExplorerCacheRunsActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(RefreshSourceExplorerCache, workflow.RegisterOptions{Name: RefreshSourceExplorerCacheWorkflowName})
	env.RegisterActivityWithOptions(func(input RefreshSourceExplorerCacheInput) (RefreshSourceExplorerCacheResult, error) {
		require.Equal(t, RefreshSourceExplorerCacheInput{
			ActionRunID: "action-run-1",
			SourceName:  "finland_prhytj",
			Trigger:     "manual",
		}, input)
		return RefreshSourceExplorerCacheResult{
			ActionRunID: "action-run-1",
			SourceName:  "finland_prhytj",
			CacheTable:  "corpscout_sources.fi_prhytj_company_explorer_cache",
			Rows:        2,
			RefreshedAt: "2026-06-10T10:00:00Z",
		}, nil
	}, activity.RegisterOptions{Name: RefreshSourceExplorerCacheActivityName})

	env.ExecuteWorkflow(RefreshSourceExplorerCacheWorkflowName, RefreshSourceExplorerCacheInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result RefreshSourceExplorerCacheResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, uint64(2), result.Rows)
	require.Equal(t, "corpscout_sources.fi_prhytj_company_explorer_cache", result.CacheTable)
}

func TestMapSourceIndustriesToNACERunsActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(MapSourceIndustriesToNACE, workflow.RegisterOptions{Name: MapSourceIndustriesToNACEWorkflowName})
	env.RegisterActivityWithOptions(func(input MapSourceIndustriesToNACEInput) (MapSourceIndustriesToNACEResult, error) {
		require.Equal(t, MapSourceIndustriesToNACEInput{
			ActionRunID: "action-run-1",
			SourceName:  "finland_prhytj",
			Trigger:     "manual",
		}, input)
		return MapSourceIndustriesToNACEResult{
			ActionRunID:  "action-run-1",
			SourceName:   "finland_prhytj",
			MappingTable: "corpscout_sources.fi_prhytj_industry_nace_mappings",
			Rows:         3,
			MappedRows:   2,
			UnmappedRows: 1,
			MappedAt:     "2026-06-10T10:00:00Z",
		}, nil
	}, activity.RegisterOptions{Name: MapSourceIndustriesToNACEActivityName})

	env.ExecuteWorkflow(MapSourceIndustriesToNACEWorkflowName, MapSourceIndustriesToNACEInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result MapSourceIndustriesToNACEResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, uint64(2), result.MappedRows)
	require.Equal(t, "corpscout_sources.fi_prhytj_industry_nace_mappings", result.MappingTable)
}

func TestDownloadSourceRejectsPreparedActionRunMismatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(DownloadSource, workflow.RegisterOptions{Name: DownloadSourceWorkflowName})
	env.RegisterActivityWithOptions(func(input SyncSourceDownloadInput) (PrepareSourceDownloadResult, error) {
		require.Equal(t, "action-run-1", input.ActionRunID)
		return PrepareSourceDownloadResult{ActionRunID: "different-action-run"}, nil
	}, activity.RegisterOptions{Name: PrepareSourceDownloadActivityName})

	env.ExecuteWorkflow(DownloadSourceWorkflowName, SyncSourceDownloadInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "prepared action run different-action-run does not match input action run action-run-1")
}

func TestSyncSourceToClickHouseRequiresActionRunIDs(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncSourceToClickHouse, workflow.RegisterOptions{Name: SyncSourceToClickHouseWorkflowName})

	env.ExecuteWorkflow(SyncSourceToClickHouseWorkflowName, SyncSourceToClickHouseInput{
		SourceName: "finland_prhytj",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "download_action_run_id is required")
}
