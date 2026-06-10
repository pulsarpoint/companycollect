package companysources

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	SourceTaskQueue = "corpscout-company-sources"

	DownloadSourceWorkflowName           = "CompanySourceDownloadWorkflow"
	DownloadSourceFileWorkflowName       = "CompanySourceDownloadFileWorkflow"
	ImportSourceToClickHouseWorkflowName = "CompanySourceClickHouseImportWorkflow"
	SyncSourceToClickHouseWorkflowName   = "CompanySourceSyncClickHouseWorkflow"

	PrepareSourceDownloadActivityName    = "PrepareSourceDownloadActivity"
	FinishSourceDownloadActivityName     = "FinishSourceDownloadActivity"
	DownloadSourceFileActivityName       = "DownloadSourceFileActivity"
	ImportSourceToClickHouseActivityName = "ImportSourceToClickHouseActivity"

	ActionPullSource       = "pull_source"
	ActionImportClickHouse = "import_clickhouse"
	StatusSucceeded        = "succeeded"
	StatusFailed           = "failed"
)

func ActionRunWorkflowID(actionRunID string) string {
	return "company-source-action-run-" + actionRunID
}

func FileRunWorkflowID(fileRunID string) string {
	return "company-source-file-run-" + fileRunID
}

type SyncSourceDownloadInput struct {
	ActionRunID string `json:"action_run_id"`
	SourceName  string `json:"source_name"`
	Trigger     string `json:"trigger"`
}

type DownloadSourceFileInput struct {
	FileRunID         string `json:"file_run_id"`
	SourceName        string `json:"source_name"`
	FileKey           string `json:"file_key"`
	Trigger           string `json:"trigger"`
	ParentActionRunID string `json:"parent_action_run_id,omitempty"`
}

type DownloadSourceFileResult struct {
	FileRunID          string `json:"file_run_id"`
	SourceName         string `json:"source_name"`
	FileKey            string `json:"file_key"`
	Path               string `json:"path"`
	ContentSHA256      string `json:"content_sha256"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	RecordsWritten     int64  `json:"records_written"`
}

type DownloadedSourceFileSummary struct {
	FileKey        string `json:"file_key"`
	FileRunID      string `json:"file_run_id"`
	Path           string `json:"path"`
	RecordsWritten int64  `json:"records_written"`
}

type PrepareSourceDownloadResult struct {
	ActionRunID string                    `json:"action_run_id"`
	Files       []DownloadSourceFileInput `json:"files"`
}

type FinishSourceDownloadInput struct {
	ActionRunID  string                        `json:"action_run_id"`
	Status       string                        `json:"status"`
	Files        []DownloadedSourceFileSummary `json:"files"`
	ErrorMessage string                        `json:"error_message"`
}

type ImportSourceToClickHouseInput struct {
	ActionRunID         string   `json:"action_run_id"`
	SourceName          string   `json:"source_name"`
	Trigger             string   `json:"trigger"`
	DownloadActionRunID string   `json:"download_action_run_id,omitempty"`
	FileRunIDs          []string `json:"file_run_ids,omitempty"`
	BatchSize           int      `json:"batch_size"`
	Limit               int64    `json:"limit"`
}

type SyncSourceToClickHouseInput struct {
	DownloadActionRunID string `json:"download_action_run_id,omitempty"`
	ImportActionRunID   string `json:"import_action_run_id,omitempty"`
	SourceName          string `json:"source_name"`
	Trigger             string `json:"trigger"`
	BatchSize           int    `json:"batch_size"`
	Limit               int64  `json:"limit"`
}

type DownloadSourceResult struct {
	ActionRunID string                        `json:"action_run_id"`
	Files       []DownloadedSourceFileSummary `json:"files"`
}

type ImportSourceToClickHouseResult struct {
	ActionRunID    string   `json:"action_run_id"`
	ImportedTables []string `json:"imported_tables"`
	ImportedRows   int64    `json:"imported_rows"`
}

type SyncSourceToClickHouseResult struct {
	Download DownloadSourceResult           `json:"download"`
	Import   ImportSourceToClickHouseResult `json:"import"`
}

func DownloadSource(ctx workflow.Context, input SyncSourceDownloadInput) (DownloadSourceResult, error) {
	ctx = withSourceActivityOptions(ctx, 60*time.Minute)
	var prepared PrepareSourceDownloadResult
	if err := workflow.ExecuteActivity(ctx, PrepareSourceDownloadActivityName, input).Get(ctx, &prepared); err != nil {
		return DownloadSourceResult{}, errors.Wrap(err, "prepare source download")
	}
	actionRunID := prepared.ActionRunID
	if actionRunID == "" {
		actionRunID = input.ActionRunID
	}
	if actionRunID == "" {
		return DownloadSourceResult{}, errors.New("action_run_id is required")
	}
	if input.ActionRunID != "" && prepared.ActionRunID != "" && prepared.ActionRunID != input.ActionRunID {
		return DownloadSourceResult{}, errors.Errorf("prepared action run %s does not match input action run %s", prepared.ActionRunID, input.ActionRunID)
	}

	summaries := make([]DownloadedSourceFileSummary, 0, len(prepared.Files))
	var runErr error
	for _, file := range prepared.Files {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: FileRunWorkflowID(file.FileRunID),
		})
		var downloaded DownloadSourceFileResult
		if err := workflow.ExecuteChildWorkflow(childCtx, DownloadSourceFileWorkflowName, file).Get(ctx, &downloaded); err != nil {
			runErr = errors.Wrapf(err, "download source file %s", file.FileKey)
			break
		}
		summaries = append(summaries, DownloadedSourceFileSummary{
			FileKey:        downloaded.FileKey,
			FileRunID:      downloaded.FileRunID,
			Path:           downloaded.Path,
			RecordsWritten: downloaded.RecordsWritten,
		})
	}

	finish := FinishSourceDownloadInput{
		ActionRunID: actionRunID,
		Status:      StatusSucceeded,
		Files:       summaries,
	}
	if runErr != nil {
		finish.Status = StatusFailed
		finish.ErrorMessage = runErr.Error()
	}
	if err := workflow.ExecuteActivity(ctx, FinishSourceDownloadActivityName, finish).Get(ctx, nil); err != nil {
		if runErr != nil {
			return DownloadSourceResult{ActionRunID: actionRunID, Files: summaries}, errors.WithSecondaryError(runErr, err)
		}
		return DownloadSourceResult{}, errors.Wrap(err, "finish source download")
	}
	if runErr != nil {
		return DownloadSourceResult{ActionRunID: actionRunID, Files: summaries}, runErr
	}
	return DownloadSourceResult{ActionRunID: actionRunID, Files: summaries}, nil
}

func DownloadSourceFile(ctx workflow.Context, input DownloadSourceFileInput) (DownloadSourceFileResult, error) {
	ctx = withSourceActivityOptions(ctx, 60*time.Minute)
	var result DownloadSourceFileResult
	if err := workflow.ExecuteActivity(ctx, DownloadSourceFileActivityName, input).Get(ctx, &result); err != nil {
		return DownloadSourceFileResult{}, errors.Wrap(err, "download source file activity")
	}
	return result, nil
}

func ImportSourceToClickHouse(ctx workflow.Context, input ImportSourceToClickHouseInput) (ImportSourceToClickHouseResult, error) {
	ctx = withSourceActivityOptions(ctx, 2*time.Hour)
	var result ImportSourceToClickHouseResult
	if err := workflow.ExecuteActivity(ctx, ImportSourceToClickHouseActivityName, input).Get(ctx, &result); err != nil {
		return ImportSourceToClickHouseResult{}, errors.Wrap(err, "import source to clickhouse activity")
	}
	return result, nil
}

func SyncSourceToClickHouse(ctx workflow.Context, input SyncSourceToClickHouseInput) (SyncSourceToClickHouseResult, error) {
	if input.BatchSize <= 0 {
		input.BatchSize = 1000
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	if input.DownloadActionRunID == "" {
		return SyncSourceToClickHouseResult{}, errors.New("download_action_run_id is required")
	}
	if input.ImportActionRunID == "" {
		return SyncSourceToClickHouseResult{}, errors.New("import_action_run_id is required")
	}

	var downloaded DownloadSourceResult
	downloadCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: ActionRunWorkflowID(input.DownloadActionRunID),
	})
	if err := workflow.ExecuteChildWorkflow(downloadCtx, DownloadSourceWorkflowName, SyncSourceDownloadInput{
		ActionRunID: input.DownloadActionRunID,
		SourceName:  input.SourceName,
		Trigger:     input.Trigger,
	}).Get(ctx, &downloaded); err != nil {
		return SyncSourceToClickHouseResult{}, errors.Wrap(err, "download source child workflow")
	}

	fileRunIDs := make([]string, 0, len(downloaded.Files))
	for _, file := range downloaded.Files {
		if file.FileRunID != "" {
			fileRunIDs = append(fileRunIDs, file.FileRunID)
		}
	}

	var imported ImportSourceToClickHouseResult
	importCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: ActionRunWorkflowID(input.ImportActionRunID),
	})
	if err := workflow.ExecuteChildWorkflow(importCtx, ImportSourceToClickHouseWorkflowName, ImportSourceToClickHouseInput{
		ActionRunID:         input.ImportActionRunID,
		SourceName:          input.SourceName,
		Trigger:             input.Trigger,
		DownloadActionRunID: downloaded.ActionRunID,
		FileRunIDs:          fileRunIDs,
		BatchSize:           input.BatchSize,
		Limit:               input.Limit,
	}).Get(ctx, &imported); err != nil {
		return SyncSourceToClickHouseResult{}, errors.Wrap(err, "import source child workflow")
	}

	return SyncSourceToClickHouseResult{Download: downloaded, Import: imported}, nil
}

func withSourceActivityOptions(ctx workflow.Context, timeout time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	})
}
