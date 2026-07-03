package orchestration

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/client"
)

func TestTemporalWorkflowStarterSignalsSourceWorkflow(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    "translator/norway_brreg",
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, []string{"norway_brreg"}, 25, 90, 400)

	result, err := starter.StartSourceAction(context.Background(), "norway_brreg", engine.ActionRun)
	if err != nil {
		t.Fatalf("StartSourceAction() error = %v, want nil", err)
	}

	if result.WorkflowID != "translator/norway_brreg" || result.RunID != "run-123" {
		t.Fatalf("StartSourceAction() result = %#v, want workflow/run ids", result)
	}
	if temporalClient.workflowID != "translator/norway_brreg" {
		t.Fatalf("SignalWithStartWorkflow() workflow id = %q, want translator/norway_brreg", temporalClient.workflowID)
	}
	if temporalClient.signalName != engine.SignalSourceAction {
		t.Fatalf("SignalWithStartWorkflow() signal name = %q, want %q", temporalClient.signalName, engine.SignalSourceAction)
	}
	if temporalClient.signalArg != (engine.SourceActionSignal{Action: engine.ActionRun}) {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want run action", temporalClient.signalArg)
	}
	if temporalClient.options.TaskQueue != "translator-norway-brreg" {
		t.Fatalf("SignalWithStartWorkflow() task queue = %q, want translator-norway-brreg", temporalClient.options.TaskQueue)
	}
	if len(temporalClient.workflowArgs) != 1 {
		t.Fatalf("SignalWithStartWorkflow() workflow arg count = %d, want 1", len(temporalClient.workflowArgs))
	}
	want := engine.WorkflowInput{Source: "norway_brreg", BatchSize: 25, TimeoutSeconds: 90, BatchesPerRun: 400}
	if temporalClient.workflowArgs[0] != want {
		t.Fatalf("SignalWithStartWorkflow() workflow input = %#v, want %#v", temporalClient.workflowArgs[0], want)
	}
}

func TestTemporalWorkflowStarterSupportsLoadAndRunAction(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    "translator/norway_brreg",
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, []string{"norway_brreg"}, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), "norway_brreg", engine.ActionLoadAndRun); err != nil {
		t.Fatalf("StartSourceAction(load-and-run) error = %v, want nil", err)
	}

	if temporalClient.signalArg != (engine.SourceActionSignal{Action: engine.ActionLoadAndRun}) {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want load-and-run action", temporalClient.signalArg)
	}
}

func TestTemporalWorkflowStarterRejectsUnsupportedAction(t *testing.T) {
	starter := NewTemporalWorkflowStarter(&fakeTemporalClient{}, []string{"norway_brreg"}, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), "norway_brreg", "unknown"); err == nil {
		t.Fatal("StartSourceAction(unknown) error = nil, want unsupported action error")
	}
}

func TestTemporalWorkflowStarterRejectsUnknownSource(t *testing.T) {
	starter := NewTemporalWorkflowStarter(&fakeTemporalClient{}, []string{"norway_brreg"}, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), "sweden_scb", engine.ActionRun); err == nil {
		t.Fatal("StartSourceAction(unknown source) error = nil, want unsupported source error")
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
