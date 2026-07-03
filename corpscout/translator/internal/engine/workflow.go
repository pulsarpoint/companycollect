package engine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	SignalSourceAction = "source-action"

	ActionLoadAndRun = "load-and-run"
	ActionLoadQueue  = "load-queue"
	ActionRun        = "run"
)

const DefaultBatchesPerRun = 500

// WorkflowID returns the per-source workflow ID, e.g. translator/norway_brreg.
func WorkflowID(source string) string {
	return "translator/" + source
}

// TaskQueue returns the per-source task queue. Underscores become hyphens so
// norway_brreg keeps its historical queue name translator-norway-brreg.
func TaskQueue(source string) string {
	return "translator-" + strings.ReplaceAll(source, "_", "-")
}

// ActivityLoadNewInput returns the per-source LoadNewInput activity name.
func ActivityLoadNewInput(source string) string {
	return source + ".LoadNewInput"
}

// ActivityProcessOneBatch returns the per-source ProcessOneBatch activity name.
func ActivityProcessOneBatch(source string) string {
	return source + ".ProcessOneBatch"
}

// ActivityUploadOutput returns the per-source UploadOutput activity name.
func ActivityUploadOutput(source string) string {
	return source + ".UploadOutput"
}

type WorkflowInput struct {
	Source         string
	BatchSize      int
	TimeoutSeconds int
	BatchesPerRun  int
	ResumeAction   string
}

type SourceActionSignal struct {
	Action string
}

// TranslationWorkflow drives one source's translation loop: it waits for a
// source-action signal (or a ResumeAction carried across continue-as-new),
// loads new input, and processes queue batches until empty, then uploads the
// output to ClickHouse.
func TranslationWorkflow(ctx workflow.Context, input WorkflowInput) error {
	if input.Source == "" {
		return errors.New("workflow input source is required")
	}
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
		"translator workflow started",
		"source", input.Source,
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
				"translator workflow processing batch",
				"source", input.Source,
				"batch_index", batch+1,
				"batches_per_run", input.BatchesPerRun,
				"batch_size", input.BatchSize,
			)
			var processResult ProcessResult
			if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch(input.Source), processInput).Get(ctx, &processResult); err != nil {
				return err
			}
			logger.Info(
				"translator workflow batch processed",
				"source", input.Source,
				"batch_index", batch+1,
				"translated_count", processResult.TranslatedCount,
				"pending_count", processResult.PendingCount,
				"output_count", processResult.OutputCount,
			)
			if processResult.PendingCount == 0 {
				if processResult.OutputCount == 0 {
					logger.Info("translator workflow queue is empty", "source", input.Source)
					return nil
				}
				var uploadResult UploadResult
				logger.Info("translator workflow uploading output", "source", input.Source, "output_count", processResult.OutputCount)
				return workflow.ExecuteActivity(ctx, ActivityUploadOutput(input.Source)).Get(ctx, &uploadResult)
			}
		}

		nextInput := input
		nextInput.ResumeAction = ActionRun
		logger.Info(
			"translator workflow continuing as new",
			"source", input.Source,
			"batches_per_run", input.BatchesPerRun,
			"next_resume_action", nextInput.ResumeAction,
		)
		return workflow.NewContinueAsNewError(ctx, TranslationWorkflow, nextInput)
	}

	signalChannel := workflow.GetSignalChannel(ctx, SignalSourceAction)
	signal := SourceActionSignal{Action: input.ResumeAction}
	if signal.Action == "" {
		logger.Info("translator workflow waiting for source action signal", "source", input.Source, "signal_name", SignalSourceAction)
		signalChannel.Receive(ctx, &signal)
	}
	logger.Info("translator workflow source action received", "source", input.Source, "action", signal.Action)

	for {
		logger.Info("translator workflow action started", "source", input.Source, "action", signal.Action)
		switch signal.Action {
		case ActionLoadAndRun:
			var result LoadResult
			if err := workflow.ExecuteActivity(ctx, ActivityLoadNewInput(input.Source)).Get(ctx, &result); err != nil {
				return err
			}
			if err := processUntilEmpty(); err != nil {
				return err
			}
		case ActionLoadQueue:
			var result LoadResult
			if err := workflow.ExecuteActivity(ctx, ActivityLoadNewInput(input.Source)).Get(ctx, &result); err != nil {
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
			logger.Info("translator workflow completed action", "source", input.Source, "action", signal.Action)
			return nil
		}
		logger.Info("translator workflow source action received", "source", input.Source, "action", signal.Action)
	}
}
