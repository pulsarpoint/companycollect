package orchestration

import (
	"context"
	"fmt"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"go.temporal.io/sdk/client"
)

type temporalSignalStarter interface {
	SignalWithStartWorkflow(
		ctx context.Context,
		workflowID string,
		signalName string,
		signalArg interface{},
		options client.StartWorkflowOptions,
		workflow interface{},
		workflowArgs ...interface{},
	) (client.WorkflowRun, error)
}

type WorkflowActionResult struct {
	WorkflowID string
	RunID      string
}

type TemporalWorkflowStarter struct {
	client         temporalSignalStarter
	taskQueue      string
	batchSize      int
	timeoutSeconds int
	batchesPerRun  int
}

func NewTemporalWorkflowStarter(
	temporalClient temporalSignalStarter,
	taskQueue string,
	batchSize int,
	timeoutSeconds int,
	batchesPerRun int,
) *TemporalWorkflowStarter {
	return &TemporalWorkflowStarter{
		client:         temporalClient,
		taskQueue:      taskQueue,
		batchSize:      batchSize,
		timeoutSeconds: timeoutSeconds,
		batchesPerRun:  batchesPerRun,
	}
}

func (s *TemporalWorkflowStarter) StartSourceAction(
	ctx context.Context,
	source string,
	action string,
) (WorkflowActionResult, error) {
	if s.client == nil {
		return WorkflowActionResult{}, fmt.Errorf("temporal client is required")
	}
	if s.taskQueue == "" {
		return WorkflowActionResult{}, fmt.Errorf("temporal task queue is required")
	}
	if source != brreg.SourceName {
		return WorkflowActionResult{}, fmt.Errorf("unsupported source: %s", source)
	}

	workflowAction, err := normalizeWorkflowAction(action)
	if err != nil {
		return WorkflowActionResult{}, err
	}

	run, err := s.client.SignalWithStartWorkflow(
		ctx,
		brreg.WorkflowID,
		brreg.SignalSourceAction,
		brreg.SourceActionSignal{Action: workflowAction},
		client.StartWorkflowOptions{
			ID:        brreg.WorkflowID,
			TaskQueue: s.taskQueue,
		},
		brreg.NorwayBRREGWorkflow,
		brreg.WorkflowInput{
			BatchSize:      s.batchSize,
			TimeoutSeconds: s.timeoutSeconds,
			BatchesPerRun:  s.batchesPerRun,
		},
	)
	if err != nil {
		return WorkflowActionResult{}, fmt.Errorf("signal/start workflow: %w", err)
	}

	return WorkflowActionResult{
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
	}, nil
}

func normalizeWorkflowAction(action string) (string, error) {
	switch action {
	case brreg.ActionLoadAndRun:
		return brreg.ActionLoadAndRun, nil
	case brreg.ActionLoadQueue:
		return brreg.ActionLoadQueue, nil
	case brreg.ActionRun:
		return brreg.ActionRun, nil
	default:
		return "", fmt.Errorf("unsupported action: %s", action)
	}
}
