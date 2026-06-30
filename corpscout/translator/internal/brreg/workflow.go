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

const DefaultBatchesPerRun = 500

type WorkflowInput struct {
	BatchSize      int
	TimeoutSeconds int
	BatchesPerRun  int
	ResumeAction   string
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
	if input.BatchesPerRun <= 0 {
		input.BatchesPerRun = DefaultBatchesPerRun
	}
	logger := workflow.GetLogger(ctx)
	logger.Info(
		"brreg workflow started",
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"batches_per_run", input.BatchesPerRun,
		"resume_action", input.ResumeAction,
	)

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

		for batch := 0; batch < input.BatchesPerRun; batch++ {
			logger.Info(
				"brreg workflow processing batch",
				"batch_index", batch+1,
				"batches_per_run", input.BatchesPerRun,
				"batch_size", input.BatchSize,
			)
			var processResult ProcessResult
			if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch, processInput).Get(ctx, &processResult); err != nil {
				return err
			}
			logger.Info(
				"brreg workflow batch processed",
				"batch_index", batch+1,
				"translated_count", processResult.TranslatedCount,
				"pending_count", processResult.PendingCount,
				"output_count", processResult.OutputCount,
			)
			if processResult.PendingCount == 0 {
				if processResult.OutputCount == 0 {
					logger.Info("brreg workflow queue is empty")
					return nil
				}
				var uploadResult UploadResult
				logger.Info("brreg workflow uploading output", "output_count", processResult.OutputCount)
				return workflow.ExecuteActivity(ctx, ActivityUploadOutput).Get(ctx, &uploadResult)
			}
		}

		nextInput := input
		nextInput.ResumeAction = ActionRun
		logger.Info(
			"brreg workflow continuing as new",
			"batches_per_run", input.BatchesPerRun,
			"next_resume_action", nextInput.ResumeAction,
		)
		return workflow.NewContinueAsNewError(ctx, NorwayBRREGWorkflow, nextInput)
	}

	signalChannel := workflow.GetSignalChannel(ctx, SignalSourceAction)
	signal := SourceActionSignal{Action: input.ResumeAction}
	if signal.Action == "" {
		logger.Info("brreg workflow waiting for source action signal", "signal_name", SignalSourceAction)
		signalChannel.Receive(ctx, &signal)
	}
	logger.Info("brreg workflow source action received", "action", signal.Action)

	for {
		logger.Info("brreg workflow action started", "action", signal.Action)
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
			logger.Info("brreg workflow completed action", "action", signal.Action)
			return nil
		}
		logger.Info("brreg workflow source action received", "action", signal.Action)
	}
}
