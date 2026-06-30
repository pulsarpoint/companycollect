package orchestration

import (
	"fmt"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowIDNorwayBRREG = "translator/norway_brreg"

	SignalSourceAction = "source-action"

	ActionLoadQueue = "load-queue"
	ActionRun       = "run"

	ActivityLoadNewInput    = "brreg.LoadNewInput"
	ActivityProcessOneBatch = "brreg.ProcessOneBatch"
	ActivityUploadOutput    = "brreg.UploadOutput"
)

type WorkflowInput struct {
	BatchSize      int
	TimeoutSeconds int
}

type SourceActionSignal struct {
	Action string
}

func NorwayBRREGWorkflow(ctx workflow.Context, input WorkflowInput) error {
	if input.BatchSize <= 0 {
		input.BatchSize = 50
	}
	if input.TimeoutSeconds <= 0 {
		input.TimeoutSeconds = 120
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 10,
		},
	})

	signalChannel := workflow.GetSignalChannel(ctx, SignalSourceAction)
	var signal SourceActionSignal
	signalChannel.Receive(ctx, &signal)

	for {
		if err := handleSourceAction(ctx, signal.Action, input); err != nil {
			return err
		}

		if !signalChannel.ReceiveAsync(&signal) {
			return nil
		}
	}
}

func handleSourceAction(ctx workflow.Context, action string, input WorkflowInput) error {
	switch action {
	case ActionLoadQueue:
		var result brreg.InitResult
		return workflow.ExecuteActivity(ctx, ActivityLoadNewInput).Get(ctx, &result)
	case ActionRun:
		return processUntilEmpty(ctx, input)
	default:
		return fmt.Errorf("unsupported source action: %s", action)
	}
}

func processUntilEmpty(ctx workflow.Context, input WorkflowInput) error {
	processInput := brreg.ProcessInput{
		BatchSize:      input.BatchSize,
		TimeoutSeconds: input.TimeoutSeconds,
	}

	for {
		var processResult brreg.ProcessResult
		if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch, processInput).Get(ctx, &processResult); err != nil {
			return err
		}
		if processResult.TranslatedCount > 0 {
			continue
		}

		var uploadResult brreg.UploadResult
		return workflow.ExecuteActivity(ctx, ActivityUploadOutput).Get(ctx, &uploadResult)
	}
}
