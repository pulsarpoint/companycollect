package orchestration

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"go.temporal.io/sdk/client"
)

func TestTemporalWorkflowStarterSignalsNorwayBRREGWorkflow(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    brreg.WorkflowID,
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, brreg.TaskQueue, 25, 90, 400)

	result, err := starter.StartSourceAction(context.Background(), brreg.SourceName, brreg.ActionRun)
	if err != nil {
		t.Fatalf("StartSourceAction() error = %v, want nil", err)
	}

	if result.WorkflowID != brreg.WorkflowID || result.RunID != "run-123" {
		t.Fatalf("StartSourceAction() result = %#v, want workflow/run ids", result)
	}
	if temporalClient.workflowID != brreg.WorkflowID {
		t.Fatalf("SignalWithStartWorkflow() workflow id = %q, want %q", temporalClient.workflowID, brreg.WorkflowID)
	}
	if temporalClient.signalName != brreg.SignalSourceAction {
		t.Fatalf("SignalWithStartWorkflow() signal name = %q, want %q", temporalClient.signalName, brreg.SignalSourceAction)
	}
	if temporalClient.signalArg != (brreg.SourceActionSignal{Action: brreg.ActionRun}) {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want run action", temporalClient.signalArg)
	}
	if temporalClient.options.TaskQueue != brreg.TaskQueue {
		t.Fatalf("SignalWithStartWorkflow() task queue = %q, want %q", temporalClient.options.TaskQueue, brreg.TaskQueue)
	}
	if len(temporalClient.workflowArgs) != 1 {
		t.Fatalf("SignalWithStartWorkflow() workflow arg count = %d, want 1", len(temporalClient.workflowArgs))
	}
	if temporalClient.workflowArgs[0] != (brreg.WorkflowInput{BatchSize: 25, TimeoutSeconds: 90, BatchesPerRun: 400}) {
		t.Fatalf("SignalWithStartWorkflow() workflow input = %#v, want configured input", temporalClient.workflowArgs[0])
	}
}

func TestTemporalWorkflowStarterSupportsLoadAndRunAction(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    brreg.WorkflowID,
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, brreg.TaskQueue, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), brreg.SourceName, brreg.ActionLoadAndRun); err != nil {
		t.Fatalf("StartSourceAction(load-and-run) error = %v, want nil", err)
	}

	if temporalClient.signalArg != (brreg.SourceActionSignal{Action: brreg.ActionLoadAndRun}) {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want load-and-run action", temporalClient.signalArg)
	}
}

func TestTemporalWorkflowStarterRejectsUnsupportedAction(t *testing.T) {
	starter := NewTemporalWorkflowStarter(&fakeTemporalClient{}, brreg.TaskQueue, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), brreg.SourceName, "unknown"); err == nil {
		t.Fatal("StartSourceAction(unknown) error = nil, want unsupported action error")
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
