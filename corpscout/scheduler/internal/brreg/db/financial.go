package brregdb

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/financial"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultFinancialLeaseSeconds int32 = 900
	defaultFinancialBatchLimit   int32 = 25
)

func (g *Gateway) ClaimSourceFinancialBatch(
	ctx context.Context,
	command ClaimSourceFinancialBatchCommand,
) (ClaimSourceFinancialBatchResult, error) {
	if g.pool == nil {
		return ClaimSourceFinancialBatchResult{}, errors.New("brreg workflow database pool not available")
	}
	limit := command.Limit
	if limit <= 0 {
		limit = defaultFinancialBatchLimit
	}
	maxAttempts := command.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = g.maxAttempts
	}
	leaseSeconds := command.LeaseSeconds
	if leaseSeconds <= 0 {
		leaseSeconds = defaultFinancialLeaseSeconds
	}
	var workerID *string
	if command.WorkerID != "" {
		workerID = &command.WorkerID
	}

	companies, err := db.New(g.pool).ClaimBrregCompanyFinancialBatch(ctx, db.ClaimBrregCompanyFinancialBatchParams{
		MaxParallelTasks: command.MaxParallelTasks,
		Limit:            limit,
		MaxAttempts:      maxAttempts,
		WorkerID:         workerID,
		LeaseSeconds:     leaseSeconds,
	})
	if err != nil {
		return ClaimSourceFinancialBatchResult{}, errors.Wrap(err, "claim brreg company financial batch")
	}
	return ClaimSourceFinancialBatchResult{
		StatusRowsInserted: 0,
		Companies:          companies,
	}, nil
}

func (g *Gateway) StoreSourceFinancialStatements(
	ctx context.Context,
	command StoreSourceFinancialStatementsCommand,
) (StoreSourceFinancialStatementsResult, error) {
	if command.CompanyID == uuid.Nil {
		return StoreSourceFinancialStatementsResult{}, errors.New("brreg source financial company id is required")
	}
	if command.RawRecordID == uuid.Nil {
		return StoreSourceFinancialStatementsResult{}, errors.New("brreg source financial raw record id is required")
	}
	metadata, err := financialMetadata(command.Metadata)
	if err != nil {
		return StoreSourceFinancialStatementsResult{}, err
	}
	trigger := nilIfEmpty(command.Trigger)
	var upserted int32
	err = g.withTx(ctx, func(queries *db.Queries) error {
		for _, statement := range command.Statements {
			params, err := financialStatementParams(command.CompanyID, command.RawRecordID, statement, metadata, trigger)
			if err != nil {
				return err
			}
			if _, err := queries.UpsertBrregSourceFinancialStatement(ctx, params); err != nil {
				return errors.Wrap(err, "upsert brreg source financial statement")
			}
			upserted++
		}
		if _, err := queries.MarkBrregCompanyFinancialSucceeded(ctx, db.MarkBrregCompanyFinancialSucceededParams{
			Metadata:  metadata,
			CompanyID: command.CompanyID,
		}); err != nil {
			return errors.Wrap(err, "mark brreg source financial succeeded")
		}
		return nil
	})
	if err != nil {
		return StoreSourceFinancialStatementsResult{}, err
	}
	return StoreSourceFinancialStatementsResult{StatementsUpserted: upserted}, nil
}

func (g *Gateway) MarkSourceFinancialSkipped(ctx context.Context, command MarkSourceFinancialSkippedCommand) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	metadata, err := financialMetadata(command.Metadata)
	if err != nil {
		return err
	}
	_, err = db.New(g.pool).MarkBrregCompanyFinancialSkipped(ctx, db.MarkBrregCompanyFinancialSkippedParams{
		Metadata:  metadata,
		CompanyID: command.CompanyID,
	})
	return errors.Wrap(err, "mark brreg source financial skipped")
}

func (g *Gateway) MarkSourceFinancialFailed(ctx context.Context, command MarkSourceFinancialFailedCommand) error {
	if g.pool == nil {
		return errors.New("brreg workflow database pool not available")
	}
	metadata, err := financialMetadata(command.Metadata)
	if err != nil {
		return err
	}
	maxAttempts := command.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = g.maxAttempts
	}
	_, err = db.New(g.pool).MarkBrregCompanyFinancialFailed(ctx, db.MarkBrregCompanyFinancialFailedParams{
		Terminal:      command.Terminal,
		MaxAttempts:   maxAttempts,
		Error:         command.Error,
		ErrorCategory: nilIfEmpty(command.ErrorCategory),
		ErrorCode:     nilIfEmpty(command.ErrorCode),
		RetryStrategy: nilIfEmpty(command.RetryStrategy),
		Metadata:      metadata,
		CompanyID:     command.CompanyID,
	})
	return errors.Wrap(err, "mark brreg source financial failed")
}

func financialStatementParams(
	companyID uuid.UUID,
	rawRecordID uuid.UUID,
	statement financial.Statement,
	metadata json.RawMessage,
	trigger *string,
) (db.UpsertBrregSourceFinancialStatementParams, error) {
	facts, err := marshalFinancialObject(statement.Facts)
	if err != nil {
		return db.UpsertBrregSourceFinancialStatementParams{}, errors.Wrap(err, "marshal brreg source financial facts")
	}
	evidence, err := marshalFinancialObject(statement.Evidence)
	if err != nil {
		return db.UpsertBrregSourceFinancialStatementParams{}, errors.Wrap(err, "marshal brreg source financial evidence")
	}
	statementMetadata, err := marshalFinancialObject(statement.Metadata)
	if err != nil {
		return db.UpsertBrregSourceFinancialStatementParams{}, errors.Wrap(err, "marshal brreg source financial metadata")
	}
	if string(metadata) != jsonPayloadEmptyObject {
		var merged map[string]json.RawMessage
		if err := json.Unmarshal(statementMetadata, &merged); err != nil {
			return db.UpsertBrregSourceFinancialStatementParams{}, errors.Wrap(err, "decode brreg source financial metadata")
		}
		merged["workflow"] = metadata
		statementMetadata, err = marshalFinancialObject(merged)
		if err != nil {
			return db.UpsertBrregSourceFinancialStatementParams{}, errors.Wrap(err, "merge brreg source financial metadata")
		}
	}
	rawPayload := json.RawMessage(statement.RawPayload)
	if len(rawPayload) == 0 || string(rawPayload) == "null" {
		rawPayload = json.RawMessage(jsonPayloadEmptyObject)
	}
	sourceURL := nilIfEmpty(statement.Evidence.SourceURL)
	return db.UpsertBrregSourceFinancialStatementParams{
		CompanyID:                         companyID,
		RawRecordID:                       rawRecordID,
		FiscalYear:                        statement.FiscalYear,
		PeriodStart:                       nilIfEmpty(statement.PeriodStart),
		PeriodEnd:                         nilIfEmpty(statement.PeriodEnd),
		StatementType:                     statement.StatementType,
		IsConsolidated:                    statement.IsConsolidated,
		OriginalCurrency:                  nilIfEmpty(statement.OriginalCurrency),
		RevenueOriginalAmount:             statement.RevenueOriginalAmount,
		OperatingIncomeOriginalAmount:     statement.OperatingIncomeOriginalAmount,
		OperatingProfitOriginalAmount:     statement.OperatingProfitOriginalAmount,
		ProfitBeforeTaxOriginalAmount:     statement.ProfitBeforeTaxOriginalAmount,
		NetIncomeOriginalAmount:           statement.NetIncomeOriginalAmount,
		TotalAssetsOriginalAmount:         statement.TotalAssetsOriginalAmount,
		CurrentAssetsOriginalAmount:       statement.CurrentAssetsOriginalAmount,
		FixedAssetsOriginalAmount:         statement.FixedAssetsOriginalAmount,
		TotalEquityOriginalAmount:         statement.TotalEquityOriginalAmount,
		TotalLiabilitiesOriginalAmount:    statement.TotalLiabilitiesOriginalAmount,
		CurrentLiabilitiesOriginalAmount:  statement.CurrentLiabilitiesOriginalAmount,
		LongTermLiabilitiesOriginalAmount: statement.LongTermLiabilitiesOriginalAmount,
		SourceUrl:                         sourceURL,
		Facts:                             facts,
		Evidence:                          evidence,
		RawFinancialPayload:               rawPayload,
		Metadata:                          statementMetadata,
		Trigger:                           trigger,
	}, nil
}

func financialMetadata(metadata map[string]any) (json.RawMessage, error) {
	if len(metadata) == 0 {
		return json.RawMessage(jsonPayloadEmptyObject), nil
	}
	return marshalFinancialObject(metadata)
}

func marshalFinancialObject(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || string(data) == "null" {
		return json.RawMessage(jsonPayloadEmptyObject), nil
	}
	return json.RawMessage(data), nil
}
