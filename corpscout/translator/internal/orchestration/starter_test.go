package orchestration

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/client"
)

func TestTemporalWorkflowStarterSignalsProcessWorkflow(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    "translator/process",
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, 25, 90, 400, 7)

	result, err := starter.StartProcess(context.Background())
	if err != nil {
		t.Fatalf("StartProcess() error = %v, want nil", err)
	}

	if result.WorkflowID != "translator/process" || result.RunID != "run-123" {
		t.Fatalf("StartProcess() result = %#v, want workflow/run ids", result)
	}
	if temporalClient.workflowID != engine.ProcessWorkflowID {
		t.Fatalf("SignalWithStartWorkflow() workflow id = %q, want %q", temporalClient.workflowID, engine.ProcessWorkflowID)
	}
	if temporalClient.signalName != engine.SignalNewItems {
		t.Fatalf("SignalWithStartWorkflow() signal name = %q, want %q", temporalClient.signalName, engine.SignalNewItems)
	}
	if temporalClient.signalArg != nil {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want nil", temporalClient.signalArg)
	}
	if temporalClient.options.ID != engine.ProcessWorkflowID {
		t.Fatalf("SignalWithStartWorkflow() options.ID = %q, want %q", temporalClient.options.ID, engine.ProcessWorkflowID)
	}
	if temporalClient.options.TaskQueue != engine.ProcessTaskQueue {
		t.Fatalf("SignalWithStartWorkflow() task queue = %q, want %q", temporalClient.options.TaskQueue, engine.ProcessTaskQueue)
	}
	if len(temporalClient.workflowArgs) != 1 {
		t.Fatalf("SignalWithStartWorkflow() workflow arg count = %d, want 1", len(temporalClient.workflowArgs))
	}
	want := engine.WorkflowInput{BatchSize: 25, TimeoutSeconds: 90, BatchesPerRun: 400, FlushEveryBatches: 7}
	if temporalClient.workflowArgs[0] != want {
		t.Fatalf("SignalWithStartWorkflow() workflow input = %#v, want %#v", temporalClient.workflowArgs[0], want)
	}
}

func TestTemporalWorkflowStarterRequiresClient(t *testing.T) {
	starter := NewTemporalWorkflowStarter(nil, 25, 90, 400, 7)

	if _, err := starter.StartProcess(context.Background()); err == nil {
		t.Fatal("StartProcess() error = nil, want temporal client required error")
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
