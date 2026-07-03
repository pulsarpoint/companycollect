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
	client            temporalSignalStarter
	batchSize         int
	timeoutSeconds    int
	batchesPerRun     int
	flushEveryBatches int
}

// NewTemporalWorkflowStarter builds a starter that can signal-with-start the
// single translation workflow.
func NewTemporalWorkflowStarter(
	temporalClient temporalSignalStarter,
	batchSize int,
	timeoutSeconds int,
	batchesPerRun int,
	flushEveryBatches int,
) *TemporalWorkflowStarter {
	return &TemporalWorkflowStarter{
		client:            temporalClient,
		batchSize:         batchSize,
		timeoutSeconds:    timeoutSeconds,
		batchesPerRun:     batchesPerRun,
		flushEveryBatches: flushEveryBatches,
	}
}

func (s *TemporalWorkflowStarter) StartProcess(ctx context.Context) (WorkflowActionResult, error) {
	if s.client == nil {
		return WorkflowActionResult{}, fmt.Errorf("temporal client is required")
	}
	run, err := s.client.SignalWithStartWorkflow(
		ctx,
		engine.ProcessWorkflowID,
		engine.SignalNewItems,
		nil,
		client.StartWorkflowOptions{
			ID:        engine.ProcessWorkflowID,
			TaskQueue: engine.ProcessTaskQueue,
		},
		engine.TranslationWorkflow,
		engine.WorkflowInput{
			BatchSize:         s.batchSize,
			TimeoutSeconds:    s.timeoutSeconds,
			BatchesPerRun:     s.batchesPerRun,
			FlushEveryBatches: s.flushEveryBatches,
		},
	)
	if err != nil {
		return WorkflowActionResult{}, fmt.Errorf("signal/start workflow: %w", err)
	}
	return WorkflowActionResult{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}
