package api

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
	"go.temporal.io/sdk/client"
)

func TestTemporalWorkflowStarterSignalsNorwayBRREGWorkflow(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    orchestration.WorkflowIDNorwayBRREG,
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, "translator-task-queue", 25, 90)

	result, err := starter.StartSourceAction(context.Background(), "norway_brreg", "run")
	if err != nil {
		t.Fatalf("start source action: %v", err)
	}

	if result.WorkflowID != orchestration.WorkflowIDNorwayBRREG || result.RunID != "run-123" {
		t.Fatalf("unexpected workflow result: %#v", result)
	}
	if temporalClient.workflowID != orchestration.WorkflowIDNorwayBRREG {
		t.Fatalf("unexpected workflow id: %s", temporalClient.workflowID)
	}
	if temporalClient.signalName != orchestration.SignalSourceAction {
		t.Fatalf("unexpected signal name: %s", temporalClient.signalName)
	}
	if temporalClient.signalArg != (orchestration.SourceActionSignal{Action: orchestration.ActionRun}) {
		t.Fatalf("unexpected signal arg: %#v", temporalClient.signalArg)
	}
	if temporalClient.options.TaskQueue != "translator-task-queue" {
		t.Fatalf("unexpected task queue: %s", temporalClient.options.TaskQueue)
	}
	if len(temporalClient.workflowArgs) != 1 {
		t.Fatalf("expected one workflow arg, got %d", len(temporalClient.workflowArgs))
	}
	if temporalClient.workflowArgs[0] != (orchestration.WorkflowInput{BatchSize: 25, TimeoutSeconds: 90}) {
		t.Fatalf("unexpected workflow input: %#v", temporalClient.workflowArgs[0])
	}
}

func TestTemporalWorkflowStarterRejectsUnsupportedAction(t *testing.T) {
	starter := NewTemporalWorkflowStarter(&fakeTemporalClient{}, "translator-task-queue", 25, 90)

	if _, err := starter.StartSourceAction(context.Background(), "norway_brreg", "unknown"); err == nil {
		t.Fatal("expected unsupported action error")
	}
}

type fakeTemporalClient struct {
	workflowID   string
	signalName   string
	signalArg    interface{}
	options      client.StartWorkflowOptions
	workflowArgs []interface{}
	run          client.WorkflowRun
	err          error
}

func (f *fakeTemporalClient) SignalWithStartWorkflow(
	ctx context.Context,
	workflowID string,
	signalName string,
	signalArg interface{},
	options client.StartWorkflowOptions,
	workflow interface{},
	workflowArgs ...interface{},
) (client.WorkflowRun, error) {
	f.workflowID = workflowID
	f.signalName = signalName
	f.signalArg = signalArg
	f.options = options
	f.workflowArgs = workflowArgs
	return f.run, f.err
}

type fakeWorkflowRun struct {
	id    string
	runID string
}

func (f fakeWorkflowRun) GetID() string {
	return f.id
}

func (f fakeWorkflowRun) GetRunID() string {
	return f.runID
}

func (f fakeWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}

func (f fakeWorkflowRun) GetWithOptions(
	ctx context.Context,
	valuePtr interface{},
	options client.WorkflowRunGetOptions,
) error {
	return nil
}
