package workflow

import (
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/france/actions"
)

const (
	LoadFranceBulkRawRecordsTaskQueue    = "france-bulk-ingest"
	LoadFranceBulkRawRecordsWorkflowName = "LoadFranceBulkRawRecords"

	ProcessFranceSireneStockUniteLegaleWorkflowName   = "ProcessFranceSireneStockUniteLegale"
	ProcessFranceSireneStockEtablissementWorkflowName = "ProcessFranceSireneStockEtablissement"
	stageFranceBulkRawFilesActivity                   = "StageFranceBulkRawFilesActivity"
	processFranceSireneStockUniteLegaleActivity       = "ProcessFranceSireneStockUniteLegaleActivity"
	processFranceSireneStockEtablissementActivity     = "ProcessFranceSireneStockEtablissementActivity"
	finishFranceBulkRawIngestActivity                 = "FinishFranceBulkRawIngestActivity"
	markFranceBulkRawIngestFailedActivity             = "MarkFranceBulkRawIngestFailedActivity"
	loadFranceBulkRawRecordsHeartbeatTimeout          = 15 * time.Minute
	markFranceBulkRawIngestFailedStartTimeout         = time.Minute
)

type StageFranceBulkRawFilesActivityInput = actions.StageFranceBulkRawFilesActivityInput
type StageFranceBulkRawFilesActivityResult = actions.StageFranceBulkRawFilesActivityResult
type ProcessFranceSireneDatasetResult = actions.ProcessFranceSireneDatasetResult
type ProcessFranceSireneStockUniteLegaleActivityInput = actions.ProcessFranceSireneStockUniteLegaleActivityInput
type ProcessFranceSireneStockEtablissementActivityInput = actions.ProcessFranceSireneStockEtablissementActivityInput
type ProcessFranceSireneStockUniteLegaleInput = actions.ProcessFranceSireneStockUniteLegaleActivityInput
type ProcessFranceSireneStockEtablissementInput = actions.ProcessFranceSireneStockEtablissementActivityInput
type FinishFranceBulkRawIngestActivityInput = actions.FinishFranceBulkRawIngestActivityInput
type MarkFranceBulkRawIngestFailedActivityInput = actions.MarkFranceBulkRawIngestFailedActivityInput

type LoadFranceBulkRawRecordsInput struct {
	LegalUnitsURL     string `json:"legal_units_url,omitempty"`
	EstablishmentsURL string `json:"establishments_url,omitempty"`
	Limit             int    `json:"limit,omitempty"`
	BatchSize         int    `json:"batch_size,omitempty"`
	Trigger           string `json:"trigger,omitempty"`
}

type LoadFranceBulkRawRecordsResult struct {
	Status                     string `json:"status"`
	RowsSeen                   int32  `json:"rows_seen"`
	RowsWritten                int32  `json:"rows_written"`
	RowsInsertedNew            int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged      int32  `json:"rows_existing_unchanged"`
	RowsNewVersions            int32  `json:"rows_new_versions"`
	LegalUnitsSeen             int32  `json:"legal_units_seen"`
	LegalUnitsWritten          int32  `json:"legal_units_written"`
	EstablishmentsSeen         int32  `json:"establishments_seen"`
	EstablishmentsWritten      int32  `json:"establishments_written"`
	WorkflowRunID              string `json:"workflow_run_id,omitempty"`
	SnapshotID                 string `json:"snapshot_id,omitempty"`
	LegalUnitsSourceFileID     string `json:"legal_units_source_file_id,omitempty"`
	EstablishmentsSourceFileID string `json:"establishments_source_file_id,omitempty"`
}

func LoadFranceBulkRawRecords(
	ctx temporalworkflow.Context,
	input LoadFranceBulkRawRecordsInput,
) (LoadFranceBulkRawRecordsResult, error) {
	input = normalizeLoadFranceBulkRawRecordsInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, franceBulkActivityOptions())
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("france bulk raw ingest workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"legal_units_url", input.LegalUnitsURL,
		"establishments_url", input.EstablishmentsURL,
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"trigger", input.Trigger,
	)

	var staged StageFranceBulkRawFilesActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, stageFranceBulkRawFilesActivity, StageFranceBulkRawFilesActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		LegalUnitsURL:      input.LegalUnitsURL,
		EstablishmentsURL:  input.EstablishmentsURL,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		Trigger:            input.Trigger,
	}).Get(ctx, &staged); err != nil {
		return failFranceBulkRawIngest(ctx, logger, workflowInfo.WorkflowExecution.ID, LoadFranceBulkRawRecordsResult{}, err, "stage france bulk raw files")
	}

	result := LoadFranceBulkRawRecordsResult{
		Status:                     "running",
		WorkflowRunID:              staged.WorkflowRunID,
		SnapshotID:                 staged.SnapshotID,
		LegalUnitsSourceFileID:     staged.LegalUnitsSourceFileID,
		EstablishmentsSourceFileID: staged.EstablishmentsSourceFileID,
	}
	legalChildCtx := temporalworkflow.WithChildOptions(ctx, temporalworkflow.ChildWorkflowOptions{
		TaskQueue:  LoadFranceBulkRawRecordsTaskQueue,
		WorkflowID: childWorkflowID(workflowInfo.WorkflowExecution.ID, "process-sirene-stock-unite-legale"),
	})
	establishmentChildCtx := temporalworkflow.WithChildOptions(ctx, temporalworkflow.ChildWorkflowOptions{
		TaskQueue:  LoadFranceBulkRawRecordsTaskQueue,
		WorkflowID: childWorkflowID(workflowInfo.WorkflowExecution.ID, "process-sirene-stock-etablissement"),
	})
	legalFuture := temporalworkflow.ExecuteChildWorkflow(
		legalChildCtx,
		ProcessFranceSireneStockUniteLegale,
		ProcessFranceSireneStockUniteLegaleInput{
			SnapshotID:         staged.SnapshotID,
			SourceFileID:       staged.LegalUnitsSourceFileID,
			SourcePath:         staged.LegalUnitsPath,
			SourceURL:          staged.LegalUnitsURL,
			ResolvedURL:        staged.LegalUnitsResolvedURL,
			ContentType:        staged.LegalUnitsContentType,
			ContentLengthBytes: staged.LegalUnitsBytesDownloaded,
			ChecksumValue:      staged.LegalUnitsChecksumValue,
			Limit:              staged.Limit,
			BatchSize:          staged.BatchSize,
			Metadata:           staged.Metadata,
		},
	)
	establishmentFuture := temporalworkflow.ExecuteChildWorkflow(
		establishmentChildCtx,
		ProcessFranceSireneStockEtablissement,
		ProcessFranceSireneStockEtablissementInput{
			SnapshotID:         staged.SnapshotID,
			SourceFileID:       staged.EstablishmentsSourceFileID,
			SourcePath:         staged.EstablishmentsPath,
			SourceURL:          staged.EstablishmentsURL,
			ResolvedURL:        staged.EstablishmentsResolvedURL,
			ContentType:        staged.EstablishmentsContentType,
			ContentLengthBytes: staged.EstablishmentsBytesDownloaded,
			ChecksumValue:      staged.EstablishmentsChecksumValue,
			Limit:              staged.Limit,
			BatchSize:          staged.BatchSize,
			Metadata:           staged.Metadata,
		},
	)

	var legalResult ProcessFranceSireneDatasetResult
	if err := legalFuture.Get(ctx, &legalResult); err != nil {
		return failFranceBulkRawIngest(ctx, logger, workflowInfo.WorkflowExecution.ID, result, err, "process france sirene stock unite legale")
	}
	result.LegalUnitsSeen = legalResult.RowsSeen
	result.LegalUnitsWritten = legalResult.RowsWritten
	result.add(legalResult)

	var establishmentResult ProcessFranceSireneDatasetResult
	if err := establishmentFuture.Get(ctx, &establishmentResult); err != nil {
		return failFranceBulkRawIngest(ctx, logger, workflowInfo.WorkflowExecution.ID, result, err, "process france sirene stock etablissement")
	}
	result.EstablishmentsSeen = establishmentResult.RowsSeen
	result.EstablishmentsWritten = establishmentResult.RowsWritten
	result.add(establishmentResult)

	if err := temporalworkflow.ExecuteActivity(ctx, finishFranceBulkRawIngestActivity, FinishFranceBulkRawIngestActivityInput{
		WorkflowRunID:         staged.WorkflowRunID,
		SnapshotID:            staged.SnapshotID,
		RowsSeen:              result.RowsSeen,
		RowsWritten:           result.RowsWritten,
		RowsInsertedNew:       result.RowsInsertedNew,
		RowsExistingUnchanged: result.RowsExistingUnchanged,
		RowsNewVersions:       result.RowsNewVersions,
		Metadata:              staged.Metadata,
	}).Get(ctx, nil); err != nil {
		return failFranceBulkRawIngest(ctx, logger, workflowInfo.WorkflowExecution.ID, result, err, "finish france bulk raw ingest")
	}

	logger.Debug("france bulk raw ingest workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"rows_seen", result.RowsSeen,
		"rows_written", result.RowsWritten,
		"legal_units_seen", result.LegalUnitsSeen,
		"legal_units_written", result.LegalUnitsWritten,
		"establishments_seen", result.EstablishmentsSeen,
		"establishments_written", result.EstablishmentsWritten,
	)

	result.Status = "succeeded"
	return result, nil
}

func ProcessFranceSireneStockUniteLegale(
	ctx temporalworkflow.Context,
	input ProcessFranceSireneStockUniteLegaleInput,
) (ProcessFranceSireneDatasetResult, error) {
	ctx = temporalworkflow.WithActivityOptions(ctx, franceBulkActivityOptions())
	var result ProcessFranceSireneDatasetResult
	if err := temporalworkflow.ExecuteActivity(ctx, processFranceSireneStockUniteLegaleActivity, ProcessFranceSireneStockUniteLegaleActivityInput(input)).Get(ctx, &result); err != nil {
		return ProcessFranceSireneDatasetResult{}, errors.Wrap(err, "process france sirene stock unite legale")
	}
	return result, nil
}

func ProcessFranceSireneStockEtablissement(
	ctx temporalworkflow.Context,
	input ProcessFranceSireneStockEtablissementInput,
) (ProcessFranceSireneDatasetResult, error) {
	ctx = temporalworkflow.WithActivityOptions(ctx, franceBulkActivityOptions())
	var result ProcessFranceSireneDatasetResult
	if err := temporalworkflow.ExecuteActivity(ctx, processFranceSireneStockEtablissementActivity, ProcessFranceSireneStockEtablissementActivityInput(input)).Get(ctx, &result); err != nil {
		return ProcessFranceSireneDatasetResult{}, errors.Wrap(err, "process france sirene stock etablissement")
	}
	return result, nil
}

func franceBulkActivityOptions() temporalworkflow.ActivityOptions {
	return temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 12 * time.Hour,
		HeartbeatTimeout:    loadFranceBulkRawRecordsHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    30 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Minute,
			MaximumAttempts:    3,
		},
	}
}

func failFranceBulkRawIngest(
	ctx temporalworkflow.Context,
	logger temporallog.Logger,
	temporalWorkflowID string,
	result LoadFranceBulkRawRecordsResult,
	cause error,
	contextMessage string,
) (LoadFranceBulkRawRecordsResult, error) {
	if markErr := markFranceBulkRawIngestFailed(ctx, temporalWorkflowID, cause); markErr != nil {
		logger.Error("mark france bulk raw ingest workflow failed",
			"temporal_workflow_id", temporalWorkflowID,
			"error", markErr,
		)
	}
	result.Status = "failed"
	return result, errors.Wrap(cause, contextMessage)
}

func markFranceBulkRawIngestFailed(ctx temporalworkflow.Context, temporalWorkflowID string, cause error) error {
	failureCtx := temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: markFranceBulkRawIngestFailedStartTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	})
	return temporalworkflow.ExecuteActivity(
		failureCtx,
		markFranceBulkRawIngestFailedActivity,
		MarkFranceBulkRawIngestFailedActivityInput{
			TemporalWorkflowID: temporalWorkflowID,
			Error:              cause.Error(),
		},
	).Get(failureCtx, nil)
}

func normalizeLoadFranceBulkRawRecordsInput(input LoadFranceBulkRawRecordsInput) LoadFranceBulkRawRecordsInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func (r *LoadFranceBulkRawRecordsResult) add(result ProcessFranceSireneDatasetResult) {
	r.RowsSeen += result.RowsSeen
	r.RowsWritten += result.RowsWritten
	r.RowsInsertedNew += result.RowsInsertedNew
	r.RowsExistingUnchanged += result.RowsExistingUnchanged
	r.RowsNewVersions += result.RowsNewVersions
}

func childWorkflowID(parentWorkflowID string, suffix string) string {
	parentWorkflowID = strings.TrimSpace(parentWorkflowID)
	if parentWorkflowID == "" {
		parentWorkflowID = "france-bulk-ingest"
	}
	return parentWorkflowID + "-" + suffix
}
