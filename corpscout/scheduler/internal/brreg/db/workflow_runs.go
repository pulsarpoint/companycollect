package brregdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type workflowSelectionPayload struct {
	Source      string            `json:"source"`
	Action      string            `json:"action"`
	TaskType    string            `json:"task_type"`
	Trigger     string            `json:"trigger"`
	WorkflowID  string            `json:"workflow_id"`
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int32             `json:"limit"`
	BatchSize   int32             `json:"batch_size"`
	MaxAttempts int32             `json:"max_attempts"`
}

func (g *Gateway) PrepareWorkflow(ctx context.Context, command PrepareWorkflowCommand) (PreparedWorkflow, error) {
	if g.pool == nil {
		return PreparedWorkflow{}, errors.New("brreg workflow database pool not available")
	}
	command = normalizePrepareWorkflowCommand(command)
	if command.Source == "" {
		return PreparedWorkflow{}, errors.New("source is required")
	}
	if command.Action == "" {
		return PreparedWorkflow{}, errors.New("action is required")
	}
	if command.TaskType == "" {
		return PreparedWorkflow{}, errors.New("task type is required")
	}
	if command.WorkflowID == "" {
		return PreparedWorkflow{}, errors.New("workflow id is required")
	}

	selectionDefinition, selectionHash, err := workflowSelectionDefinition(command)
	if err != nil {
		return PreparedWorkflow{}, err
	}

	var prepared PreparedWorkflow
	err = g.withTx(ctx, func(q *db.Queries) error {
		workflowRunID, err := q.BeginBrregWorkflowRun(ctx, db.BeginBrregWorkflowRunParams{
			OrchestratorRunID: command.WorkflowID,
			RunType:           command.Action,
			Metadata:          selectionDefinition,
		})
		if err != nil {
			return errors.Wrap(err, "begin brreg workflow run")
		}

		selection, err := q.CreateBrregWorkflowTaskSelection(ctx, db.CreateBrregWorkflowTaskSelectionParams{
			TaskType:            command.TaskType.String(),
			SelectedIds:         command.IDs,
			Query:               stringFilter(command.Filters, "query", "q"),
			LifecycleState:      stringFilter(command.Filters, "state", "lifecycle_state"),
			TranslationStatus:   stringFilter(command.Filters, "translation_status"),
			DomainStatus:        stringFilter(command.Filters, "domain_status"),
			FinancialStatus:     stringFilter(command.Filters, "financial_status"),
			EnhancedStatus:      stringFilter(command.Filters, "enhanced_status"),
			MaxAttempts:         command.MaxAttempts,
			Limit:               command.Limit,
			WorkflowRunID:       workflowRunID,
			SelectionHash:       selectionHash,
			SelectionDefinition: selectionDefinition,
		})
		if err != nil {
			return errors.Wrap(err, "create brreg workflow task selection")
		}

		prepared = PreparedWorkflow{
			WorkflowRunID:   workflowRunID,
			SelectionHash:   selection.SelectionHash,
			RecordsSelected: selection.RecordsSelected,
			BatchSize:       command.BatchSize,
			MaxAttempts:     command.MaxAttempts,
		}
		return nil
	})
	if err != nil {
		return PreparedWorkflow{}, err
	}
	return prepared, nil
}

func (g *Gateway) FinishWorkflowRun(ctx context.Context, command FinishWorkflowRunCommand) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	_, err := db.New(g.pool).FinishBrregWorkflowRunWithStats(ctx, db.FinishBrregWorkflowRunWithStatsParams{
		Status:           command.Status.String(),
		RecordsSeen:      command.RecordsSeen,
		RecordsCompleted: command.RecordsCompleted,
		RecordsFailed:    command.RecordsFailed,
		Error:            command.Error,
		ID:               command.WorkflowRunID,
	})
	if err != nil {
		return errors.Wrap(err, "finish brreg workflow run")
	}
	return nil
}

func (g *Gateway) FailRunningTasksForWorkflowRun(ctx context.Context, command FinishWorkflowRunCommand) (int32, error) {
	if g.pool == nil {
		return 0, errors.New("brreg workflow database pool not available")
	}
	maxAttempts := command.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = g.maxAttempts
	}
	failedTasks, err := db.New(g.pool).FailRunningBrregWorkflowTasksForRun(ctx, db.FailRunningBrregWorkflowTasksForRunParams{
		MaxAttempts:   maxAttempts,
		Error:         command.Error,
		WorkflowRunID: command.WorkflowRunID,
	})
	if err != nil {
		return 0, errors.Wrap(err, "fail running brreg workflow tasks for run")
	}
	return failedTasks, nil
}

func workflowSelectionDefinition(command PrepareWorkflowCommand) ([]byte, string, error) {
	normalized := normalizePrepareWorkflowCommand(command)
	payload := workflowSelectionPayload{
		Source:      normalized.Source,
		Action:      normalized.Action,
		TaskType:    normalized.TaskType.String(),
		Trigger:     normalized.Trigger,
		WorkflowID:  normalized.WorkflowID,
		IDs:         append([]string(nil), normalized.IDs...),
		Filters:     normalized.Filters,
		Limit:       normalized.Limit,
		BatchSize:   normalized.BatchSize,
		MaxAttempts: normalized.MaxAttempts,
	}
	sort.Strings(payload.IDs)

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, "", errors.Wrap(err, "marshal brreg workflow selection definition")
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func normalizePrepareWorkflowCommand(command PrepareWorkflowCommand) PrepareWorkflowCommand {
	if command.Trigger == "" {
		command.Trigger = "manual"
	}
	if command.Limit <= 0 {
		command.Limit = command.DefaultLimit
	}
	if command.BatchSize <= 0 {
		command.BatchSize = command.DefaultBatchSize
	}
	if command.MaxAttempts <= 0 {
		command.MaxAttempts = command.DefaultMaxAttempts
	}
	if command.Filters == nil {
		command.Filters = map[string]string{}
	}
	return command
}

func stringFilter(filters map[string]string, keys ...string) *string {
	if filters == nil {
		return nil
	}
	for _, key := range keys {
		value := filters[key]
		if value != "" {
			return &value
		}
	}
	return nil
}
