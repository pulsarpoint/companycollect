package brregdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) RecordDomainActionSuccess(ctx context.Context, command RecordDomainActionSuccessCommand) error {
	return g.withTx(ctx, func(q *db.Queries) error {
		attemptID, err := createDomainActionAttempt(ctx, q, domainActionAttemptCommand{
			WorkflowRunID: command.WorkflowRunID,
			TaskAttemptID: command.TaskAttemptID,
			RawRecordID:   command.RawRecordID,
			ActionType:    command.ActionType,
			Provider:      command.Provider,
			Model:         command.Model,
			Input:         command.Input,
			Attempt:       command.Attempt,
			Metadata:      command.Metadata,
		})
		if err != nil {
			return err
		}
		if err := q.InsertBrregDomainActionArtifact(ctx, db.InsertBrregDomainActionArtifactParams{
			AttemptID:    attemptID,
			RawRecordID:  command.RawRecordID,
			ArtifactType: command.ArtifactType.String(),
			Payload:      jsonObject(command.ArtifactPayload),
			PayloadHash:  hashJSON(command.ArtifactPayload),
			Metadata:     jsonObject(command.Metadata),
		}); err != nil {
			return errors.Wrap(err, "insert brreg domain action artifact")
		}
		return finishDomainActionAttempt(ctx, q, attemptID, domainActionAttemptFinishCommand{
			Status:   "succeeded",
			Metadata: command.Metadata,
		})
	})
}

func (g *Gateway) RecordDomainActionFailure(ctx context.Context, command RecordDomainActionFailureCommand) error {
	return g.withTx(ctx, func(q *db.Queries) error {
		attemptID, err := createDomainActionAttempt(ctx, q, domainActionAttemptCommand{
			WorkflowRunID: command.WorkflowRunID,
			TaskAttemptID: command.TaskAttemptID,
			RawRecordID:   command.RawRecordID,
			ActionType:    command.ActionType,
			Provider:      command.Provider,
			Model:         command.Model,
			Input:         command.Input,
			Attempt:       command.Attempt,
			Metadata:      command.Metadata,
		})
		if err != nil {
			return err
		}
		return finishDomainActionAttempt(ctx, q, attemptID, domainActionAttemptFinishCommand{
			Status:        "failed",
			Error:         command.Error,
			ErrorCategory: command.ErrorCategory,
			ErrorCode:     command.ErrorCode,
			RetryStrategy: command.RetryStrategy,
			Metadata:      command.Metadata,
		})
	})
}

type domainActionAttemptCommand struct {
	WorkflowRunID uuid.UUID
	TaskAttemptID uuid.UUID
	RawRecordID   uuid.UUID
	ActionType    DomainActionType
	Provider      string
	Model         string
	Input         json.RawMessage
	Attempt       int32
	Metadata      json.RawMessage
}

type domainActionAttemptFinishCommand struct {
	Status        string
	Error         string
	ErrorCategory string
	ErrorCode     string
	RetryStrategy string
	Metadata      json.RawMessage
}

func createDomainActionAttempt(
	ctx context.Context,
	q *db.Queries,
	command domainActionAttemptCommand,
) (uuid.UUID, error) {
	attemptID, err := q.CreateBrregDomainActionAttempt(ctx, db.CreateBrregDomainActionAttemptParams{
		WorkflowRunID: uuidAsNullable(command.WorkflowRunID),
		TaskAttemptID: uuidAsNullable(command.TaskAttemptID),
		RawRecordID:   command.RawRecordID,
		ActionType:    command.ActionType.String(),
		Provider:      stringPtrOrNil(command.Provider),
		Model:         stringPtrOrNil(command.Model),
		InputHash:     hashJSON(command.Input),
		Attempt:       normalizedDomainActionAttempt(command.Attempt),
		Metadata:      jsonObject(command.Metadata),
	})
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "create brreg domain action attempt")
	}
	return attemptID, nil
}

func finishDomainActionAttempt(
	ctx context.Context,
	q *db.Queries,
	attemptID uuid.UUID,
	command domainActionAttemptFinishCommand,
) error {
	params := db.FinishBrregDomainActionAttemptParams{
		ID:       attemptID,
		Status:   command.Status,
		Metadata: jsonObject(command.Metadata),
	}
	if command.Error != "" {
		params.Error = &command.Error
	}
	if command.ErrorCategory != "" {
		params.ErrorCategory = &command.ErrorCategory
	}
	if command.ErrorCode != "" {
		params.ErrorCode = &command.ErrorCode
	}
	if command.RetryStrategy != "" {
		params.RetryStrategy = &command.RetryStrategy
	}
	if err := q.FinishBrregDomainActionAttempt(ctx, params); err != nil {
		return errors.Wrap(err, "finish brreg domain action attempt")
	}
	return nil
}

func normalizedDomainActionAttempt(value int32) int32 {
	if value <= 0 {
		return 1
	}
	return value
}

func hashJSON(payload json.RawMessage) string {
	payload = jsonObject(payload)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func stringPtrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func uuidAsNullable(value uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: value, Valid: value != uuid.Nil}
}
