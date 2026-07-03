package orchestration

import (
	"context"
	"fmt"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
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
	sources        map[string]bool
	batchSize      int
	timeoutSeconds int
	batchesPerRun  int
}

// NewTemporalWorkflowStarter builds a starter that can signal-with-start the
// translation workflow for any of the configured sources.
func NewTemporalWorkflowStarter(
	temporalClient temporalSignalStarter,
	sources []string,
	batchSize int,
	timeoutSeconds int,
	batchesPerRun int,
) *TemporalWorkflowStarter {
	known := make(map[string]bool, len(sources))
	for _, source := range sources {
		known[source] = true
	}
	return &TemporalWorkflowStarter{
		client:         temporalClient,
		sources:        known,
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
	if !s.sources[source] {
		return WorkflowActionResult{}, fmt.Errorf("unsupported source: %s", source)
	}

	workflowAction, err := normalizeWorkflowAction(action)
	if err != nil {
		return WorkflowActionResult{}, err
	}

	run, err := s.client.SignalWithStartWorkflow(
		ctx,
		engine.WorkflowID(source),
		engine.SignalSourceAction,
		engine.SourceActionSignal{Action: workflowAction},
		client.StartWorkflowOptions{
			ID:        engine.WorkflowID(source),
			TaskQueue: engine.TaskQueue(source),
		},
		engine.TranslationWorkflow,
		engine.WorkflowInput{
			Source:         source,
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
	case engine.ActionLoadAndRun, engine.ActionLoadQueue, engine.ActionRun:
		return action, nil
	default:
		return "", fmt.Errorf("unsupported action: %s", action)
	}
}
