package brregdb

import (
	"context"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) ClaimTranslationBatch(ctx context.Context, command ClaimTaskBatchCommand) ([]db.ClaimBrregWorkflowTaskSelectionBatchRow, error) {
	return g.claimTaskBatch(ctx, TaskTypeTranslate, command)
}

func (g *Gateway) ClaimDomainBatch(ctx context.Context, command ClaimTaskBatchCommand) ([]db.ClaimBrregWorkflowTaskSelectionBatchRow, error) {
	return g.claimTaskBatch(ctx, TaskTypeDiscoverDomains, command)
}

func (g *Gateway) ClaimFinancialBatch(ctx context.Context, command ClaimTaskBatchCommand) ([]db.ClaimBrregWorkflowTaskSelectionBatchRow, error) {
	return g.claimTaskBatch(ctx, TaskTypeConvertFinancials, command)
}

func (g *Gateway) claimTaskBatch(ctx context.Context, taskType TaskType, command ClaimTaskBatchCommand) ([]db.ClaimBrregWorkflowTaskSelectionBatchRow, error) {
	if command.BatchSize <= 0 {
		return nil, errors.New("batch size must be positive")
	}
	if command.MaxParallelTasks <= 0 {
		return nil, errors.New("max parallel tasks must be positive")
	}
	if command.LeaseSeconds <= 0 {
		return nil, errors.New("lease seconds must be positive")
	}
	if command.SelectionHash == "" {
		return nil, errors.New("selection hash is required when claiming a brreg task batch")
	}
	if command.WorkflowRunID == nil {
		return nil, errors.New("workflow run id is required when claiming a selected brreg task batch")
	}
	if g.pool == nil {
		return nil, errors.New("brreg workflow database pool not available")
	}
	return g.claimTaskSelectionBatch(ctx, taskType, command)
}

func (g *Gateway) claimTaskSelectionBatch(ctx context.Context, taskType TaskType, command ClaimTaskBatchCommand) ([]db.ClaimBrregWorkflowTaskSelectionBatchRow, error) {
	maxAttempts := command.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = g.maxAttempts
	}
	rows, err := db.New(g.pool).ClaimBrregWorkflowTaskSelectionBatch(ctx, db.ClaimBrregWorkflowTaskSelectionBatchParams{
		TaskType:         taskType.String(),
		MaxParallelTasks: command.MaxParallelTasks,
		SelectionHash:    command.SelectionHash,
		BatchSize:        command.BatchSize,
		MaxAttempts:      maxAttempts,
		LeaseSeconds:     command.LeaseSeconds,
		WorkflowRunID:    *command.WorkflowRunID,
		WorkerID:         command.WorkerID,
		Metadata:         jsonObject(command.Metadata),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "claim selected brreg %s task batch", taskType)
	}
	return rows, nil
}
