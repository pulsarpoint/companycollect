package api

import (
	"context"
	"fmt"

	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
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

type TemporalWorkflowStarter struct {
	client         temporalSignalStarter
	taskQueue      string
	batchSize      int
	timeoutSeconds int
}

func NewTemporalWorkflowStarter(
	temporalClient temporalSignalStarter,
	taskQueue string,
	batchSize int,
	timeoutSeconds int,
) *TemporalWorkflowStarter {
	return &TemporalWorkflowStarter{
		client:         temporalClient,
		taskQueue:      taskQueue,
		batchSize:      batchSize,
		timeoutSeconds: timeoutSeconds,
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
	if source != "norway_brreg" {
		return WorkflowActionResult{}, fmt.Errorf("unsupported source: %s", source)
	}

	workflowAction, err := normalizeWorkflowAction(action)
	if err != nil {
		return WorkflowActionResult{}, err
	}

	run, err := s.client.SignalWithStartWorkflow(
		ctx,
		orchestration.WorkflowIDNorwayBRREG,
		orchestration.SignalSourceAction,
		orchestration.SourceActionSignal{Action: workflowAction},
		client.StartWorkflowOptions{
			ID:        orchestration.WorkflowIDNorwayBRREG,
			TaskQueue: s.taskQueue,
		},
		orchestration.NorwayBRREGWorkflow,
		orchestration.WorkflowInput{
			BatchSize:      s.batchSize,
			TimeoutSeconds: s.timeoutSeconds,
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
	case "load-queue":
		return orchestration.ActionLoadQueue, nil
	case "run":
		return orchestration.ActionRun, nil
	default:
		return "", fmt.Errorf("unsupported action: %s", action)
	}
}
