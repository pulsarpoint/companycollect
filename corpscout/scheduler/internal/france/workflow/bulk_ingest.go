package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/france/actions"
)

const (
	LoadFranceBulkRawRecordsTaskQueue    = "france-bulk-ingest"
	LoadFranceBulkRawRecordsWorkflowName = "LoadFranceBulkRawRecords"

	loadFranceBulkRawRecordsActivity = "LoadFranceBulkRawRecordsActivity"
)

type LoadFranceBulkRawRecordsActivityInput = actions.LoadFranceBulkRawRecordsActivityInput
type LoadFranceBulkRawRecordsActivityResult = actions.LoadFranceBulkRawRecordsActivityResult

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
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 12 * time.Hour,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    30 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Minute,
			MaximumAttempts:    3,
		},
	})
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

	var activityResult LoadFranceBulkRawRecordsActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, loadFranceBulkRawRecordsActivity, LoadFranceBulkRawRecordsActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		LegalUnitsURL:      input.LegalUnitsURL,
		EstablishmentsURL:  input.EstablishmentsURL,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return LoadFranceBulkRawRecordsResult{}, errors.Wrap(err, "load france bulk raw records")
	}

	logger.Debug("france bulk raw ingest workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"rows_seen", activityResult.RowsSeen,
		"rows_written", activityResult.RowsWritten,
		"legal_units_seen", activityResult.LegalUnitsSeen,
		"legal_units_written", activityResult.LegalUnitsWritten,
		"establishments_seen", activityResult.EstablishmentsSeen,
		"establishments_written", activityResult.EstablishmentsWritten,
	)

	return LoadFranceBulkRawRecordsResult{
		Status:                     "succeeded",
		RowsSeen:                   activityResult.RowsSeen,
		RowsWritten:                activityResult.RowsWritten,
		RowsInsertedNew:            activityResult.RowsInsertedNew,
		RowsExistingUnchanged:      activityResult.RowsExistingUnchanged,
		RowsNewVersions:            activityResult.RowsNewVersions,
		LegalUnitsSeen:             activityResult.LegalUnitsSeen,
		LegalUnitsWritten:          activityResult.LegalUnitsWritten,
		EstablishmentsSeen:         activityResult.EstablishmentsSeen,
		EstablishmentsWritten:      activityResult.EstablishmentsWritten,
		WorkflowRunID:              activityResult.WorkflowRunID,
		SnapshotID:                 activityResult.SnapshotID,
		LegalUnitsSourceFileID:     activityResult.LegalUnitsSourceFileID,
		EstablishmentsSourceFileID: activityResult.EstablishmentsSourceFileID,
	}, nil
}

func normalizeLoadFranceBulkRawRecordsInput(input LoadFranceBulkRawRecordsInput) LoadFranceBulkRawRecordsInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}
