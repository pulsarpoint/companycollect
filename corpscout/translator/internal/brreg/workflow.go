package brreg

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	SourceName = "norway_brreg"

	WorkflowID = "translator/norway_brreg"
	TaskQueue  = "translator-norway-brreg"

	SignalSourceAction = "source-action"

	ActionLoadAndRun = "load-and-run"
	ActionLoadQueue  = "load-queue"
	ActionRun        = "run"

	ActivityLoadNewInput    = "brreg.LoadNewInput"
	ActivityProcessOneBatch = "brreg.ProcessOneBatch"
	ActivityUploadOutput    = "brreg.UploadOutput"
)

const DefaultMaxBatchesPerRun = 500

type WorkflowInput struct {
	BatchSize        int
	TimeoutSeconds   int
	MaxBatchesPerRun int
	ResumeAction     string
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
	if input.MaxBatchesPerRun <= 0 {
		input.MaxBatchesPerRun = DefaultMaxBatchesPerRun
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 10,
		},
	})

	processUntilEmpty := func() error {
		processInput := ProcessInput{
			BatchSize:      input.BatchSize,
			TimeoutSeconds: input.TimeoutSeconds,
		}

		batchesProcessed := 0
		for {
			var processResult ProcessResult
			if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch, processInput).Get(ctx, &processResult); err != nil {
				return err
			}
			if processResult.TranslatedCount == 0 {
				var uploadResult UploadResult
				return workflow.ExecuteActivity(ctx, ActivityUploadOutput).Get(ctx, &uploadResult)
			}

			batchesProcessed++
			if batchesProcessed >= input.MaxBatchesPerRun {
				nextInput := input
				nextInput.ResumeAction = ActionRun
				return workflow.NewContinueAsNewError(ctx, NorwayBRREGWorkflow, nextInput)
			}
		}
	}

	signalChannel := workflow.GetSignalChannel(ctx, SignalSourceAction)
	signal := SourceActionSignal{Action: input.ResumeAction}
	if signal.Action == "" {
		signalChannel.Receive(ctx, &signal)
	}

	for {
		switch signal.Action {
		case ActionLoadAndRun:
			var result InitResult
			if err := workflow.ExecuteActivity(ctx, ActivityLoadNewInput).Get(ctx, &result); err != nil {
				return err
			}
			if err := processUntilEmpty(); err != nil {
				return err
			}
		case ActionLoadQueue:
			var result InitResult
			if err := workflow.ExecuteActivity(ctx, ActivityLoadNewInput).Get(ctx, &result); err != nil {
				return err
			}
		case ActionRun:
			if err := processUntilEmpty(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported source action: %s", signal.Action)
		}

		if !signalChannel.ReceiveAsync(&signal) {
			return nil
		}
	}
}
