package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const defaultSEOutputDir = "/var/lib/data-pipelines/results/se"

func PullSE(ctx workflow.Context, input contracts.PullSEInput) (contracts.PullCompaniesResult, error) {
	runIDStr := genRunID(ctx, input.RunID)
	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = defaultSEOutputDir
	}
	mode := input.Mode
	if mode == "" {
		mode = "full"
	} else if mode != "full" && mode != "bulk" {
		return contracts.PullCompaniesResult{}, unsupportedSourceModeError("se", mode)
	}

	pythonCtx := workflow.WithActivityOptions(ctx, sourceDownloadActivityOptions())
	importCtx := workflow.WithActivityOptions(ctx, sourceImportActivityOptions())
	markCtx := workflow.WithActivityOptions(ctx, markCompleteActivityOptions())

	var download contracts.DownloadSourceFilesResult
	if err := workflow.ExecuteActivity(pythonCtx, "download_se_hvd_dataset", contracts.DownloadSourceFilesInput{
		Source:    "se",
		Mode:      mode,
		OutputDir: outputDir,
	}).Get(ctx, &download); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	var goAct *activities.GoActivities
	var written int
	if err := workflow.ExecuteActivity(importCtx, goAct.ImportSEHVDBulk, contracts.ImportSEHVDBulkParams{
		Files:          download.Files,
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
	}).Get(ctx, &written); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	result := contracts.PullCompaniesResult{
		RecordsWritten: written,
		PagesFetched:   sourceFilePageCount(download.Files),
	}

	if err := workflow.ExecuteActivity(markCtx, goAct.MarkExecutionComplete, contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         "se",
		Country:        "SE",
		Result:         result,
		FinalCursor:    mode + ":" + download.SnapshotID,
	}).Get(ctx, nil); err != nil {
		return result, err
	}

	return result, nil
}
