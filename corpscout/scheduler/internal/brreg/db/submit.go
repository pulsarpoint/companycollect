package brregdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) SubmitTranslationResult(ctx context.Context, command SubmitTranslationResultCommand) error {
	return g.submitTaskResult(ctx, submitTaskResultCommand{
		TaskAttemptID:     command.Result.TaskAttemptID,
		ResultStatus:      command.Result.Status,
		ResultError:       command.Result.Error,
		Failure:           command.Failure,
		MaxAttempts:       command.MaxAttempts,
		StatusForResult:   taskAttemptStatusForTranslation,
		InsertErrorReason: "insert brreg translation result",
		Insert: func(q *db.Queries) error {
			return q.InsertBrregWorkflowTranslationResult(ctx, command.Result)
		},
	})
}

func (g *Gateway) SubmitDomainResult(ctx context.Context, command SubmitDomainResultCommand) error {
	return g.submitTaskResult(ctx, submitTaskResultCommand{
		TaskAttemptID:     command.Result.TaskAttemptID,
		ResultStatus:      command.Result.Status,
		ResultError:       command.Result.Error,
		Failure:           command.Failure,
		MaxAttempts:       command.MaxAttempts,
		StatusForResult:   taskAttemptStatusForDomain,
		InsertErrorReason: "insert brreg domain result",
		Insert: func(q *db.Queries) error {
			return q.InsertBrregWorkflowDomainResult(ctx, command.Result)
		},
	})
}

func (g *Gateway) SubmitFinancialResult(ctx context.Context, command SubmitFinancialResultCommand) error {
	return g.submitTaskResult(ctx, submitTaskResultCommand{
		TaskAttemptID:     command.Result.TaskAttemptID,
		ResultStatus:      command.Result.Status,
		ResultError:       command.Result.Error,
		Failure:           command.Failure,
		MaxAttempts:       command.MaxAttempts,
		StatusForResult:   taskAttemptStatusForFinancial,
		InsertErrorReason: "insert brreg financial result",
		Insert: func(q *db.Queries) error {
			return q.InsertBrregWorkflowFinancialResult(ctx, command.Result)
		},
	})
}

type submitTaskResultCommand struct {
	TaskAttemptID     uuid.UUID
	ResultStatus      string
	ResultError       *string
	Failure           *TaskFailure
	MaxAttempts       int32
	StatusForResult   func(string) (string, error)
	Insert            func(*db.Queries) error
	InsertErrorReason string
}

func (g *Gateway) submitTaskResult(ctx context.Context, command submitTaskResultCommand) error {
	status, err := command.StatusForResult(command.ResultStatus)
	if err != nil {
		return err
	}
	maxAttempts := command.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = g.maxAttempts
	}
	return g.withTx(ctx, func(q *db.Queries) error {
		if err := command.Insert(q); err != nil {
			return errors.Wrap(err, command.InsertErrorReason)
		}
		return g.finishTaskAttempt(
			ctx,
			q,
			command.TaskAttemptID,
			status,
			command.ResultError,
			command.Failure,
			maxAttempts,
		)
	})
}

func (g *Gateway) finishTaskAttempt(
	ctx context.Context,
	q *db.Queries,
	taskAttemptID uuid.UUID,
	status string,
	taskError *string,
	failure *TaskFailure,
	maxAttempts int32,
) error {
	if maxAttempts <= 0 {
		maxAttempts = g.maxAttempts
	}
	params := db.FinishBrregWorkflowTaskAttemptParams{
		MaxAttempts:   maxAttempts,
		Status:        status,
		TaskAttemptID: taskAttemptID,
		Error:         taskError,
	}
	if failure != nil {
		params.ErrorCategory = &failure.ErrorCategory
		params.ErrorCode = &failure.ErrorCode
		params.RetryStrategy = &failure.RetryStrategy
	}
	if err := q.FinishBrregWorkflowTaskAttempt(ctx, params); err != nil {
		return errors.Wrap(err, "finish brreg workflow task attempt")
	}
	return nil
}

func taskAttemptStatusForTranslation(status string) (string, error) {
	switch ResultStatus(status) {
	case ResultStatusSucceeded:
		return TaskAttemptStatusSucceeded.String(), nil
	case ResultStatusSkipped:
		return TaskAttemptStatusSkipped.String(), nil
	case ResultStatusFailed:
		return TaskAttemptStatusFailed.String(), nil
	default:
		return "", errors.Newf("invalid brreg translation result status %q", status)
	}
}

func taskAttemptStatusForDomain(status string) (string, error) {
	switch ResultStatus(status) {
	case ResultStatusSucceeded, ResultStatusPartial, ResultStatusNotFound:
		return TaskAttemptStatusSucceeded.String(), nil
	case ResultStatusSkipped:
		return TaskAttemptStatusSkipped.String(), nil
	case ResultStatusFailed:
		return TaskAttemptStatusFailed.String(), nil
	default:
		return "", errors.Newf("invalid brreg domain result status %q", status)
	}
}

func taskAttemptStatusForFinancial(status string) (string, error) {
	switch ResultStatus(status) {
	case ResultStatusSucceeded:
		return TaskAttemptStatusSucceeded.String(), nil
	case ResultStatusSkipped, ResultStatusNotAvailable:
		return TaskAttemptStatusSkipped.String(), nil
	case ResultStatusFailed:
		return TaskAttemptStatusFailed.String(), nil
	default:
		return "", errors.Newf("invalid brreg financial result status %q", status)
	}
}
