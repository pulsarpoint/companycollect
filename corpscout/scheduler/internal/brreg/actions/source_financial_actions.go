package actions

import (
	"context"
	stderrors "errors"
	"log/slog"

	cerrors "github.com/cockroachdb/errors"
	"go.temporal.io/sdk/activity"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/financial"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultSourceFinancialBatchSize        int32 = 10
	defaultSourceFinancialMaxAttempts      int32 = 3
	defaultSourceFinancialLeaseSeconds     int32 = 900
	defaultSourceFinancialMaxParallelTasks int32 = 25
)

type SourceFinancialActions struct {
	gateway *brregdb.Gateway
	client  *financial.Client
}

func NewSourceFinancialActions(gateway *brregdb.Gateway, client *financial.Client) *SourceFinancialActions {
	return &SourceFinancialActions{gateway: gateway, client: client}
}

type FetchBrregSourceFinancialStatementsActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	Limit              int32  `json:"limit,omitempty"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	MaxParallelTasks   int32  `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds       int32  `json:"lease_seconds,omitempty"`
	MaxAttempts        int32  `json:"max_attempts,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type FetchBrregSourceFinancialStatementsActivityResult struct {
	RecordsClaimed     int32  `json:"records_claimed"`
	RecordsCompleted   int32  `json:"records_completed"`
	RecordsSkipped     int32  `json:"records_skipped"`
	RecordsFailed      int32  `json:"records_failed"`
	StatementsUpserted int32  `json:"statements_upserted"`
	StatusRowsInserted int32  `json:"status_rows_inserted"`
	BatchesProcessed   int32  `json:"batches_processed"`
	StoppedReason      string `json:"stopped_reason"`
}

func (a *SourceFinancialActions) FetchBrregSourceFinancialStatements(
	ctx context.Context,
	input FetchBrregSourceFinancialStatementsActivityInput,
) (FetchBrregSourceFinancialStatementsActivityResult, error) {
	if a == nil || a.gateway == nil {
		return FetchBrregSourceFinancialStatementsActivityResult{}, cerrors.New("brreg source financial gateway not available")
	}
	if a.client == nil {
		return FetchBrregSourceFinancialStatementsActivityResult{}, cerrors.New("brreg source financial client not available")
	}
	input = normalizeFetchBrregSourceFinancialStatementsInput(input)
	slog.DebugContext(ctx, "fetching brreg source financial statements",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"trigger", input.Trigger,
	)

	result := FetchBrregSourceFinancialStatementsActivityResult{StoppedReason: "unknown"}
	for {
		remaining := remainingFinancialLimit(input.Limit, result.RecordsClaimed)
		if input.Limit > 0 && remaining <= 0 {
			result.StoppedReason = "limit_reached"
			break
		}
		claimLimit := input.BatchSize
		if input.Limit > 0 && remaining < claimLimit {
			claimLimit = remaining
		}
		claim, err := a.gateway.ClaimSourceFinancialBatch(ctx, brregdb.ClaimSourceFinancialBatchCommand{
			Limit:            claimLimit,
			MaxParallelTasks: input.MaxParallelTasks,
			LeaseSeconds:     input.LeaseSeconds,
			MaxAttempts:      input.MaxAttempts,
			WorkerID:         input.TemporalWorkflowID,
		})
		if err != nil {
			return result, cerrors.Wrap(err, "claim brreg source financial batch")
		}
		result.StatusRowsInserted += claim.StatusRowsInserted
		if len(claim.Companies) == 0 {
			result.StoppedReason = "drained"
			break
		}
		result.BatchesProcessed++
		result.RecordsClaimed += int32(len(claim.Companies))
		recordFinancialHeartbeat(ctx, input, result, "batch_claimed")

		for _, company := range claim.Companies {
			outcome := a.processFinancialCompany(ctx, input, company)
			result.RecordsCompleted += outcome.recordsCompleted
			result.RecordsSkipped += outcome.recordsSkipped
			result.RecordsFailed += outcome.recordsFailed
			result.StatementsUpserted += outcome.statementsUpserted
			recordFinancialHeartbeat(ctx, input, result, "company_processed")
		}
	}
	slog.DebugContext(ctx, "fetched brreg source financial statements",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"records_claimed", result.RecordsClaimed,
		"records_completed", result.RecordsCompleted,
		"records_skipped", result.RecordsSkipped,
		"records_failed", result.RecordsFailed,
		"statements_upserted", result.StatementsUpserted,
		"status_rows_inserted", result.StatusRowsInserted,
		"batches_processed", result.BatchesProcessed,
		"stopped_reason", result.StoppedReason,
	)
	return result, nil
}

type financialCompanyOutcome struct {
	recordsCompleted   int32
	recordsSkipped     int32
	recordsFailed      int32
	statementsUpserted int32
}

func (a *SourceFinancialActions) processFinancialCompany(
	ctx context.Context,
	input FetchBrregSourceFinancialStatementsActivityInput,
	company db.ClaimBrregCompanyFinancialBatchRow,
) financialCompanyOutcome {
	recordID := company.CompanyID.String()
	lookup, err := a.client.LookupRecord(ctx, financial.LookupRecord{
		RecordID:           recordID,
		OrganizationNumber: company.OrganizationNumber,
		OrganizationName:   company.OrganizationName,
	})
	if err != nil {
		return a.markFinancialCompanyLookupFailed(ctx, input, company, err)
	}

	metadata := map[string]any{
		"workflow_id":         input.TemporalWorkflowID,
		"trigger":             input.Trigger,
		"organization_number": company.OrganizationNumber,
		"status":              lookup.Status,
	}
	switch lookup.Status {
	case financial.StatusSucceeded:
		if len(lookup.Statements) == 0 {
			metadata["reason"] = "empty_statement_list"
			if err := a.gateway.MarkSourceFinancialSkipped(ctx, brregdb.MarkSourceFinancialSkippedCommand{
				CompanyID: company.CompanyID,
				Metadata:  metadata,
			}); err != nil {
				slog.ErrorContext(ctx, "mark brreg source financial empty result skipped",
					"company_id", company.CompanyID.String(),
					"organization_number", company.OrganizationNumber,
					"error", err,
				)
				return financialCompanyOutcome{recordsFailed: 1}
			}
			return financialCompanyOutcome{recordsSkipped: 1}
		}
		stored, err := a.gateway.StoreSourceFinancialStatements(ctx, brregdb.StoreSourceFinancialStatementsCommand{
			CompanyID:   company.CompanyID,
			RawRecordID: company.RawRecordID,
			Statements:  lookup.Statements,
			Trigger:     input.Trigger,
			Metadata:    metadata,
		})
		if err != nil {
			slog.ErrorContext(ctx, "store brreg source financial statements",
				"company_id", company.CompanyID.String(),
				"organization_number", company.OrganizationNumber,
				"error", err,
			)
			return financialCompanyOutcome{recordsFailed: 1}
		}
		return financialCompanyOutcome{recordsCompleted: 1, statementsUpserted: stored.StatementsUpserted}
	case financial.StatusNotAvailable, financial.StatusUnsupportedStatementPlan:
		metadata["warnings"] = lookup.Warnings
		if err := a.gateway.MarkSourceFinancialSkipped(ctx, brregdb.MarkSourceFinancialSkippedCommand{
			CompanyID: company.CompanyID,
			Metadata:  metadata,
		}); err != nil {
			slog.ErrorContext(ctx, "mark brreg source financial skipped",
				"company_id", company.CompanyID.String(),
				"organization_number", company.OrganizationNumber,
				"status", lookup.Status,
				"error", err,
			)
			return financialCompanyOutcome{recordsFailed: 1}
		}
		return financialCompanyOutcome{recordsSkipped: 1}
	default:
		metadata["warnings"] = lookup.Warnings
		if err := a.gateway.MarkSourceFinancialFailed(ctx, brregdb.MarkSourceFinancialFailedCommand{
			CompanyID:     company.CompanyID,
			MaxAttempts:   input.MaxAttempts,
			Terminal:      false,
			Error:         "BRREG financial lookup failed",
			ErrorCategory: "brreg_financial_lookup",
			ErrorCode:     "lookup_failed",
			RetryStrategy: "retry_later",
			Metadata:      metadata,
		}); err != nil {
			slog.ErrorContext(ctx, "mark brreg source financial failed",
				"company_id", company.CompanyID.String(),
				"organization_number", company.OrganizationNumber,
				"status", lookup.Status,
				"error", err,
			)
		}
		return financialCompanyOutcome{recordsFailed: 1}
	}
}

func (a *SourceFinancialActions) markFinancialCompanyLookupFailed(
	ctx context.Context,
	input FetchBrregSourceFinancialStatementsActivityInput,
	company db.ClaimBrregCompanyFinancialBatchRow,
	err error,
) financialCompanyOutcome {
	category := "brreg_financial_lookup"
	code := "lookup_failed"
	retryStrategy := "retry_later"
	terminal := false
	var retryable *financial.RetryableError
	if stderrors.As(err, &retryable) {
		code = "brreg_retryable_error"
	} else {
		category = "brreg_financial_parse"
		code = "parse_failed"
		retryStrategy = "manual_investigation"
		terminal = true
	}
	if markErr := a.gateway.MarkSourceFinancialFailed(ctx, brregdb.MarkSourceFinancialFailedCommand{
		CompanyID:     company.CompanyID,
		MaxAttempts:   input.MaxAttempts,
		Terminal:      terminal,
		Error:         err.Error(),
		ErrorCategory: category,
		ErrorCode:     code,
		RetryStrategy: retryStrategy,
		Metadata: map[string]any{
			"workflow_id":         input.TemporalWorkflowID,
			"trigger":             input.Trigger,
			"organization_number": company.OrganizationNumber,
		},
	}); markErr != nil {
		slog.ErrorContext(ctx, "mark brreg source financial lookup failed",
			"company_id", company.CompanyID.String(),
			"organization_number", company.OrganizationNumber,
			"error", markErr,
		)
	}
	return financialCompanyOutcome{recordsFailed: 1}
}

func normalizeFetchBrregSourceFinancialStatementsInput(
	input FetchBrregSourceFinancialStatementsActivityInput,
) FetchBrregSourceFinancialStatementsActivityInput {
	if input.BatchSize <= 0 {
		input.BatchSize = defaultSourceFinancialBatchSize
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = defaultSourceFinancialMaxAttempts
	}
	if input.LeaseSeconds <= 0 {
		input.LeaseSeconds = defaultSourceFinancialLeaseSeconds
	}
	if input.MaxParallelTasks <= 0 {
		input.MaxParallelTasks = defaultSourceFinancialMaxParallelTasks
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func remainingFinancialLimit(limit int32, claimed int32) int32 {
	if limit <= 0 {
		return 0
	}
	return limit - claimed
}

func recordFinancialHeartbeat(
	ctx context.Context,
	input FetchBrregSourceFinancialStatementsActivityInput,
	result FetchBrregSourceFinancialStatementsActivityResult,
	phase string,
) {
	activity.RecordHeartbeat(ctx, map[string]any{
		"phase":                phase,
		"temporal_workflow_id": input.TemporalWorkflowID,
		"records_claimed":      result.RecordsClaimed,
		"records_completed":    result.RecordsCompleted,
		"records_skipped":      result.RecordsSkipped,
		"records_failed":       result.RecordsFailed,
		"batches_processed":    result.BatchesProcessed,
	})
}
