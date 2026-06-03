package brregdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultCompanyTranslationClaimLimit       int32 = 10
	defaultCompanyTranslationLeaseSeconds     int32 = 900
	defaultCompanyTranslationMaxParallelTasks int32 = 10
)

func (g *Gateway) ClaimCompaniesForTranslation(
	ctx context.Context,
	command ClaimCompaniesForTranslationCommand,
) (ClaimCompaniesForTranslationResult, error) {
	if g.pool == nil {
		return ClaimCompaniesForTranslationResult{}, errors.New("brreg workflow database pool not available")
	}
	command = normalizeClaimCompaniesForTranslationCommand(command)
	queries := db.New(g.pool)

	companies, err := queries.ClaimBrregCompanyTranslationBatch(ctx, claimBrregCompanyTranslationBatchParams(command))
	if err != nil {
		return ClaimCompaniesForTranslationResult{}, errors.Wrap(err, "claim brreg companies for translation")
	}
	if len(companies) > 0 {
		return ClaimCompaniesForTranslationResult{Companies: companies}, nil
	}

	inserted, err := queries.EnsureBrregCompanyProcessStatuses(ctx, command.Limit)
	if err != nil {
		return ClaimCompaniesForTranslationResult{}, errors.Wrap(err, "ensure brreg company process statuses")
	}
	if inserted == 0 {
		return ClaimCompaniesForTranslationResult{}, nil
	}

	companies, err = queries.ClaimBrregCompanyTranslationBatch(ctx, claimBrregCompanyTranslationBatchParams(command))
	if err != nil {
		return ClaimCompaniesForTranslationResult{}, errors.Wrap(err, "claim brreg companies for translation after status ensure")
	}
	return ClaimCompaniesForTranslationResult{
		StatusRowsInserted: inserted,
		Companies:          companies,
	}, nil
}

func normalizeClaimCompaniesForTranslationCommand(
	command ClaimCompaniesForTranslationCommand,
) ClaimCompaniesForTranslationCommand {
	if command.Limit <= 0 {
		command.Limit = defaultCompanyTranslationClaimLimit
	}
	if command.MaxParallelTasks <= 0 {
		command.MaxParallelTasks = defaultCompanyTranslationMaxParallelTasks
	}
	if command.LeaseSeconds <= 0 {
		command.LeaseSeconds = defaultCompanyTranslationLeaseSeconds
	}
	if command.MaxAttempts <= 0 {
		command.MaxAttempts = defaultMaxAttempts
	}
	return command
}

func claimBrregCompanyTranslationBatchParams(
	command ClaimCompaniesForTranslationCommand,
) db.ClaimBrregCompanyTranslationBatchParams {
	return db.ClaimBrregCompanyTranslationBatchParams{
		MaxParallelTasks: command.MaxParallelTasks,
		Limit:            command.Limit,
		MaxAttempts:      command.MaxAttempts,
		WorkerID:         command.WorkerID,
		LeaseSeconds:     command.LeaseSeconds,
	}
}

func (g *Gateway) MarkCompanyTranslationSucceeded(
	ctx context.Context,
	command MarkCompanyTranslationStatusCommand,
) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	if command.CompanyID == uuid.Nil {
		return errors.New("company id is required")
	}
	_, err := db.New(g.pool).MarkBrregCompanyTranslationSucceeded(ctx, db.MarkBrregCompanyTranslationSucceededParams{
		CompanyID: command.CompanyID,
		Metadata:  jsonObjectOrEmpty(command.Metadata),
	})
	if err != nil {
		return errors.Wrap(err, "mark brreg company translation succeeded")
	}
	return nil
}

func (g *Gateway) ReleaseCompanyTranslationClaim(
	ctx context.Context,
	command ReleaseCompanyTranslationClaimCommand,
) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	if command.CompanyID == uuid.Nil {
		return errors.New("company id is required")
	}
	if command.WorkerID == "" {
		return errors.New("worker id is required")
	}
	_, err := db.New(g.pool).ReleaseBrregCompanyTranslationClaim(ctx, db.ReleaseBrregCompanyTranslationClaimParams{
		CompanyID: command.CompanyID,
		WorkerID:  command.WorkerID,
	})
	if err != nil {
		return errors.Wrap(err, "release brreg company translation claim")
	}
	return nil
}

func (g *Gateway) MarkCompanyTranslationSkipped(
	ctx context.Context,
	command MarkCompanyTranslationStatusCommand,
) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	if command.CompanyID == uuid.Nil {
		return errors.New("company id is required")
	}
	_, err := db.New(g.pool).MarkBrregCompanyTranslationSkipped(ctx, db.MarkBrregCompanyTranslationSkippedParams{
		CompanyID: command.CompanyID,
		Metadata:  jsonObjectOrEmpty(command.Metadata),
	})
	if err != nil {
		return errors.Wrap(err, "mark brreg company translation skipped")
	}
	return nil
}

func (g *Gateway) MarkCompanyTranslationFailed(
	ctx context.Context,
	command MarkCompanyTranslationFailedCommand,
) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	if command.CompanyID == uuid.Nil {
		return errors.New("company id is required")
	}
	if command.MaxAttempts <= 0 {
		command.MaxAttempts = defaultMaxAttempts
	}
	_, err := db.New(g.pool).MarkBrregCompanyTranslationFailed(ctx, db.MarkBrregCompanyTranslationFailedParams{
		CompanyID:     command.CompanyID,
		Error:         command.Error,
		ErrorCategory: nilIfEmpty(command.ErrorCategory),
		ErrorCode:     nilIfEmpty(command.ErrorCode),
		RetryStrategy: nilIfEmpty(command.RetryStrategy),
		MaxAttempts:   command.MaxAttempts,
		Terminal:      command.Terminal,
		Metadata:      jsonObjectOrEmpty(command.Metadata),
	})
	if err != nil {
		return errors.Wrap(err, "mark brreg company translation failed")
	}
	return nil
}

func jsonObjectOrEmpty(value []byte) []byte {
	if len(value) == 0 || string(value) == "null" {
		return []byte(jsonPayloadEmptyObject)
	}
	return value
}
