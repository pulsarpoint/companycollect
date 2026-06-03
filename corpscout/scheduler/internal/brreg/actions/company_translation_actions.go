package actions

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
)

type CompanyTranslationActions struct {
	store *companydata.Store
}

func NewCompanyTranslationActions(store *companydata.Store) *CompanyTranslationActions {
	return &CompanyTranslationActions{store: store}
}

type ClaimBrregCompaniesForTranslationInput struct {
	Limit            int32  `json:"limit"`
	MaxParallelTasks int32  `json:"max_parallel_tasks"`
	LeaseSeconds     int32  `json:"lease_seconds"`
	MaxAttempts      int32  `json:"max_attempts"`
	WorkerID         string `json:"worker_id,omitempty"`
}

type ClaimedCompanyForTranslation struct {
	CompanyID          string `json:"company_id"`
	OrganizationNumber string `json:"organization_number"`
	OrganizationName   string `json:"organization_name"`
	AttemptCount       int32  `json:"attempt_count"`
}

type ClaimBrregCompaniesForTranslationResult struct {
	StatusRowsInserted int32                          `json:"status_rows_inserted"`
	Companies          []ClaimedCompanyForTranslation `json:"companies"`
}

type ApplyBrregCachedCompanyTranslationsInput struct {
	CompanyID     string `json:"company_id"`
	PromptVersion string `json:"prompt_version,omitempty"`
}

type ApplyBrregCachedCompanyTranslationsResult struct {
	FieldsSeen      int32 `json:"fields_seen"`
	FieldsApplied   int32 `json:"fields_applied"`
	RemainingFields int32 `json:"remaining_fields"`
}

type MarkBrregCompanyTranslationSucceededInput struct {
	CompanyID string `json:"company_id"`
	Metadata  any    `json:"metadata,omitempty"`
}

type MarkBrregCompanyTranslationSkippedInput struct {
	CompanyID string `json:"company_id"`
	Metadata  any    `json:"metadata,omitempty"`
}

type MarkBrregCompanyTranslationFailedInput struct {
	CompanyID     string `json:"company_id"`
	Error         string `json:"error"`
	ErrorCategory string `json:"error_category,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	RetryStrategy string `json:"retry_strategy,omitempty"`
	MaxAttempts   int32  `json:"max_attempts"`
	Terminal      bool   `json:"terminal"`
	Metadata      any    `json:"metadata,omitempty"`
}

func (a *CompanyTranslationActions) ClaimBrregCompaniesForTranslation(
	ctx context.Context,
	input ClaimBrregCompaniesForTranslationInput,
) (ClaimBrregCompaniesForTranslationResult, error) {
	if a == nil || a.store == nil {
		return ClaimBrregCompaniesForTranslationResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.ClaimForTranslation(ctx, companydata.ClaimForTranslationCommand{
		Limit:            input.Limit,
		MaxParallelTasks: input.MaxParallelTasks,
		LeaseSeconds:     input.LeaseSeconds,
		MaxAttempts:      input.MaxAttempts,
		WorkerID:         input.WorkerID,
	})
	if err != nil {
		return ClaimBrregCompaniesForTranslationResult{}, errors.Wrap(err, "claim brreg companies for translation")
	}
	companies := make([]ClaimedCompanyForTranslation, 0, len(result.Companies))
	for _, company := range result.Companies {
		companies = append(companies, ClaimedCompanyForTranslation{
			CompanyID:          company.Company.ID.String(),
			OrganizationNumber: company.Company.OrganizationNumber,
			OrganizationName:   company.Company.OrganizationName,
			AttemptCount:       company.AttemptCount,
		})
	}
	slog.DebugContext(ctx, "claimed brreg companies for translation",
		"status_rows_inserted", result.StatusRowsInserted,
		"companies", len(companies),
	)
	return ClaimBrregCompaniesForTranslationResult{
		StatusRowsInserted: result.StatusRowsInserted,
		Companies:          companies,
	}, nil
}

func (a *CompanyTranslationActions) ApplyBrregCachedCompanyTranslations(
	ctx context.Context,
	input ApplyBrregCachedCompanyTranslationsInput,
) (ApplyBrregCachedCompanyTranslationsResult, error) {
	if a == nil || a.store == nil {
		return ApplyBrregCachedCompanyTranslationsResult{}, errors.New("brreg companydata store not available")
	}
	companyID, err := uuid.Parse(input.CompanyID)
	if err != nil {
		return ApplyBrregCachedCompanyTranslationsResult{}, errors.Wrap(err, "parse brreg company id")
	}
	applied, err := a.store.ApplyCachedTranslations(ctx, companydata.ApplyCachedTranslationsCommand{
		CompanyID:     companyID,
		PromptVersion: input.PromptVersion,
	})
	if err != nil {
		return ApplyBrregCachedCompanyTranslationsResult{}, errors.Wrap(err, "apply cached brreg company translations")
	}
	slog.DebugContext(ctx, "applied cached brreg company translations",
		"company_id", input.CompanyID,
		"fields_seen", applied.FieldsSeen,
		"fields_applied", applied.FieldsApplied,
		"remaining_fields", applied.RemainingFields,
	)
	return ApplyBrregCachedCompanyTranslationsResult{
		FieldsSeen:      applied.FieldsSeen,
		FieldsApplied:   applied.FieldsApplied,
		RemainingFields: applied.RemainingFields,
	}, nil
}

func (a *CompanyTranslationActions) MarkBrregCompanyTranslationSucceeded(
	ctx context.Context,
	input MarkBrregCompanyTranslationSucceededInput,
) error {
	if a == nil || a.store == nil {
		return errors.New("brreg companydata store not available")
	}
	companyID, err := uuid.Parse(input.CompanyID)
	if err != nil {
		return errors.Wrap(err, "parse brreg company id")
	}
	return a.store.MarkTranslationSucceeded(ctx, companydata.MarkTranslationStatusCommand{
		CompanyID: companyID,
		Metadata:  jsonMetadata(input.Metadata),
	})
}

func (a *CompanyTranslationActions) MarkBrregCompanyTranslationSkipped(
	ctx context.Context,
	input MarkBrregCompanyTranslationSkippedInput,
) error {
	if a == nil || a.store == nil {
		return errors.New("brreg companydata store not available")
	}
	companyID, err := uuid.Parse(input.CompanyID)
	if err != nil {
		return errors.Wrap(err, "parse brreg company id")
	}
	return a.store.MarkTranslationSkipped(ctx, companydata.MarkTranslationStatusCommand{
		CompanyID: companyID,
		Metadata:  jsonMetadata(input.Metadata),
	})
}

func (a *CompanyTranslationActions) MarkBrregCompanyTranslationFailed(
	ctx context.Context,
	input MarkBrregCompanyTranslationFailedInput,
) error {
	if a == nil || a.store == nil {
		return errors.New("brreg companydata store not available")
	}
	companyID, err := uuid.Parse(input.CompanyID)
	if err != nil {
		return errors.Wrap(err, "parse brreg company id")
	}
	return a.store.MarkTranslationFailed(ctx, companydata.MarkTranslationFailedCommand{
		CompanyID:     companyID,
		Error:         input.Error,
		ErrorCategory: input.ErrorCategory,
		ErrorCode:     input.ErrorCode,
		RetryStrategy: input.RetryStrategy,
		MaxAttempts:   input.MaxAttempts,
		Terminal:      input.Terminal,
		Metadata:      jsonMetadata(input.Metadata),
	})
}

func jsonMetadata(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage(`{}`)
	}
	metadata, err := json.Marshal(value)
	if err != nil || len(metadata) == 0 || string(metadata) == "null" {
		return json.RawMessage(`{}`)
	}
	return metadata
}
