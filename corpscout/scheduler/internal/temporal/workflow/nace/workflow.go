package nace

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	domainnace "github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

const (
	SyncWorkflowName             = "SyncNACETaxonomy"
	SyncToClickHouseWorkflowName = "SyncNACEToClickHouse"
	SyncTaskQueue                = "nace-taxonomy-sync"

	SyncNACETaxonomyActivityName     = "SyncNACETaxonomyActivity"
	SyncNACEToClickHouseActivityName = "SyncNACEToClickHouseActivity"

	SyncStatusSucceeded = "succeeded"
	SyncStatusSkipped   = "skipped"
	SyncStatusFailed    = "failed"
)

type SyncNACETaxonomyInput struct {
	Revision       string `json:"revision,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type SyncNACETaxonomyActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Revision           string `json:"revision"`
	SourceURL          string `json:"source_url"`
	Trigger            string `json:"trigger"`
	ForceReprocess     bool   `json:"force_reprocess"`
}

type SyncNACETaxonomyActivityResult struct {
	Status             string `json:"status"`
	ImportRunID        string `json:"import_run_id"`
	SourceFileID       string `json:"source_file_id"`
	ContentSHA256      string `json:"content_sha256"`
	RecordsSeen        int32  `json:"records_seen"`
	RecordsImported    int32  `json:"records_imported"`
	RecordsDeactivated int32  `json:"records_deactivated"`
	Message            string `json:"message"`
}

type SyncNACETaxonomyResult = SyncNACETaxonomyActivityResult

type SyncNACEToClickHouseInput struct {
	Trigger string `json:"trigger,omitempty"`
}

type SyncNACEToClickHouseActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Trigger            string `json:"trigger"`
}

type SyncNACEToClickHouseActivityResult struct {
	Status                string `json:"status"`
	ClassificationsSynced int    `json:"classifications_synced"`
	CodesSynced           int    `json:"codes_synced"`
	AliasesSynced         int    `json:"aliases_synced"`
	Message               string `json:"message"`
}

type SyncNACEToClickHouseResult = SyncNACEToClickHouseActivityResult

func SyncNACETaxonomy(ctx temporalworkflow.Context, input SyncNACETaxonomyInput) (SyncNACETaxonomyResult, error) {
	input = normalizeSyncNACEWorkflowInput(input)
	info := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("nace taxonomy sync workflow started",
		"temporal_workflow_id", info.WorkflowExecution.ID,
		"revision", input.Revision,
		"source_url", input.SourceURL,
		"trigger", input.Trigger,
		"force_reprocess", input.ForceReprocess,
	)

	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	var result SyncNACETaxonomyActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, SyncNACETaxonomyActivityName, SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		Revision:           input.Revision,
		SourceURL:          input.SourceURL,
		Trigger:            input.Trigger,
		ForceReprocess:     input.ForceReprocess,
	}).Get(ctx, &result); err != nil {
		return SyncNACETaxonomyResult{}, errors.Wrap(err, "sync nace taxonomy activity")
	}
	logger.Debug("nace taxonomy sync workflow finished",
		"status", result.Status,
		"import_run_id", result.ImportRunID,
		"source_file_id", result.SourceFileID,
		"content_sha256", result.ContentSHA256,
		"records_seen", result.RecordsSeen,
		"records_imported", result.RecordsImported,
		"records_deactivated", result.RecordsDeactivated,
	)
	if result.Status == SyncStatusSucceeded || result.Status == SyncStatusSkipped {
		childOptions := temporalworkflow.ChildWorkflowOptions{
			WorkflowID: "nace-clickhouse-sync-" + info.WorkflowExecution.ID,
			TaskQueue:  SyncTaskQueue,
		}
		childCtx := temporalworkflow.WithChildOptions(ctx, childOptions)
		var clickHouseResult SyncNACEToClickHouseResult
		if err := temporalworkflow.ExecuteChildWorkflow(childCtx, SyncToClickHouseWorkflowName, SyncNACEToClickHouseInput{
			Trigger: "nace_taxonomy_sync",
		}).Get(childCtx, &clickHouseResult); err != nil {
			return SyncNACETaxonomyResult{}, errors.Wrap(err, "sync nace taxonomy to clickhouse child workflow")
		}
	}
	return result, nil
}

func SyncNACEToClickHouse(ctx temporalworkflow.Context, input SyncNACEToClickHouseInput) (SyncNACEToClickHouseResult, error) {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	info := temporalworkflow.GetInfo(ctx)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	var result SyncNACEToClickHouseActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, SyncNACEToClickHouseActivityName, SyncNACEToClickHouseActivityInput{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		Trigger:            input.Trigger,
	}).Get(ctx, &result); err != nil {
		return SyncNACEToClickHouseResult{}, errors.Wrap(err, "sync nace taxonomy to clickhouse activity")
	}
	return result, nil
}

func normalizeSyncNACEWorkflowInput(input SyncNACETaxonomyInput) SyncNACETaxonomyInput {
	if input.Revision == "" {
		input.Revision = domainnace.DefaultRevision
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}
