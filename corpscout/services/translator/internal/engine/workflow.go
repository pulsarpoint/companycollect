package engine

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// ProcessWorkflowID is the fixed ID of the single translation workflow.
	ProcessWorkflowID = "translator/process"
	// ProcessTaskQueue is the fixed Temporal task queue for the translator.
	ProcessTaskQueue = "translator-process"
	// SignalNewItems wakes (or signal-with-starts) the workflow after an enqueue.
	SignalNewItems = "new-items"

	ActivityProcessOneBatch = "translator.ProcessOneBatch"
	ActivityFlushOutput     = "translator.FlushOutput"
)

const DefaultBatchesPerRun = 500

type WorkflowInput struct {
	BatchSize         int
	TimeoutSeconds    int
	BatchesPerRun     int
	FlushEveryBatches int
}

// TranslationWorkflow drains the shared queue: translate batches (each batch
// is a single language pair), flush translated output to ClickHouse every
// FlushEveryBatches batches and at queue-empty, and continue-as-new after
// BatchesPerRun batches. Transient activity failures retry without an
// attempt cap (deterministic bad model output never fails the activity — it
// lands in failed_items inside ProcessOneBatch).
func TranslationWorkflow(ctx workflow.Context, input WorkflowInput) error {
	if input.BatchSize <= 0 {
		input.BatchSize = 50
	}
	if input.TimeoutSeconds <= 0 {
		input.TimeoutSeconds = 120
	}
	if input.BatchesPerRun <= 0 {
		input.BatchesPerRun = DefaultBatchesPerRun
	}
	if input.FlushEveryBatches <= 0 {
		input.FlushEveryBatches = 10
	}
	logger := workflow.GetLogger(ctx)
	logger.Info(
		"translator workflow started",
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"batches_per_run", input.BatchesPerRun,
		"flush_every_batches", input.FlushEveryBatches,
	)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
		},
	})

	signalChannel := workflow.GetSignalChannel(ctx, SignalNewItems)

	flush := func() error {
		var flushResult UploadResult
		logger.Info("translator workflow flushing output")
		if err := workflow.ExecuteActivity(ctx, ActivityFlushOutput).Get(ctx, &flushResult); err != nil {
			return err
		}
		logger.Info(
			"translator workflow flushed output",
			"rows_seen", flushResult.RowsSeen,
			"rows_inserted", flushResult.RowsInserted,
		)
		return nil
	}

	processInput := ProcessInput{
		BatchSize:      input.BatchSize,
		TimeoutSeconds: input.TimeoutSeconds,
	}
	batchesSinceFlush := 0

	for batch := 0; batch < input.BatchesPerRun; batch++ {
		var processResult ProcessResult
		if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch, processInput).Get(ctx, &processResult); err != nil {
			return err
		}
		if processResult.TranslatedCount > 0 {
			batchesSinceFlush++
		}
		logger.Info(
			"translator workflow batch processed",
			"batch_index", batch+1,
			"translated_count", processResult.TranslatedCount,
			"pending_count", processResult.PendingCount,
			"output_count", processResult.OutputCount,
		)

		if processResult.PendingCount == 0 {
			if processResult.OutputCount > 0 {
				if err := flush(); err != nil {
					return err
				}
			}
			// The queue is drained and flushed. Any signal received since the
			// last check means an enqueue may have added items after our
			// batch — loop again to re-derive pending from the queue;
			// otherwise complete. Drain extra buffered signals so one more
			// round covers any number of racing enqueues (pending is
			// re-derived from the DB, signals are only wakeups).
			if !signalChannel.ReceiveAsync(nil) {
				logger.Info("translator workflow queue is empty")
				return nil
			}
			for signalChannel.ReceiveAsync(nil) {
			}
			batchesSinceFlush = 0
			continue
		}

		if batchesSinceFlush >= input.FlushEveryBatches {
			if err := flush(); err != nil {
				return err
			}
			batchesSinceFlush = 0
		}
	}

	logger.Info("translator workflow continuing as new", "batches_per_run", input.BatchesPerRun)
	return workflow.NewContinueAsNewError(ctx, TranslationWorkflow, input)
}
