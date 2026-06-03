package brregdb

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultTermPromptVersion = "v1"
	defaultTermClaimLimit    = 100
)

func (g *Gateway) EnsurePendingTranslationTerms(
	ctx context.Context,
	command EnsurePendingTranslationTermsCommand,
) (EnsurePendingTranslationTermsResult, error) {
	if g.pool == nil {
		return EnsurePendingTranslationTermsResult{}, errors.New("brreg workflow database pool not available")
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultTermPromptVersion
	}
	inserted, err := db.New(g.pool).InsertPendingBrregTranslationTerms(ctx, db.InsertPendingBrregTranslationTermsParams{
		Provider:      nilIfEmpty(command.Provider),
		Model:         nilIfEmpty(command.Model),
		PromptVersion: command.PromptVersion,
		WorkflowID:    nilIfEmpty(command.WorkflowID),
		Limit:         nonNegativeLimit(command.Limit),
	})
	if err != nil {
		return EnsurePendingTranslationTermsResult{}, errors.Wrap(err, "insert pending brreg translation terms")
	}
	return EnsurePendingTranslationTermsResult{TermsInserted: inserted}, nil
}

func (g *Gateway) ClaimQueuedTranslationTerms(
	ctx context.Context,
	command ClaimQueuedTranslationTermsCommand,
) ([]QueuedTranslationTerm, error) {
	if g.pool == nil {
		return nil, errors.New("brreg workflow database pool not available")
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultTermPromptVersion
	}
	maxAttempts := command.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	rows, err := db.New(g.pool).MarkBrregTranslationTermsQueued(ctx, db.MarkBrregTranslationTermsQueuedParams{
		PromptVersion: command.PromptVersion,
		MaxAttempts:   maxAttempts,
		Limit:         positiveTermClaimLimit(command.Limit),
		Provider:      nilIfEmpty(command.Provider),
		Model:         nilIfEmpty(command.Model),
	})
	if err != nil {
		return nil, errors.Wrap(err, "mark brreg translation terms queued")
	}
	terms := make([]QueuedTranslationTerm, 0, len(rows))
	for _, row := range rows {
		terms = append(terms, QueuedTranslationTerm{
			ID:                   row.ID.String(),
			SourceLang:           row.SourceLang,
			TargetLang:           row.TargetLang,
			SourceTextNormalized: row.SourceTextNormalized,
			SourceText:           row.SourceText,
			TermKey:              row.TermKey,
			AttemptCount:         row.AttemptCount,
		})
	}
	return terms, nil
}

func (g *Gateway) UpsertTranslationTerms(
	ctx context.Context,
	command UpsertTranslationTermsCommand,
) (UpsertTranslationTermsResult, error) {
	if g.pool == nil {
		return UpsertTranslationTermsResult{}, errors.New("brreg workflow database pool not available")
	}
	queries := db.New(g.pool)
	for _, term := range command.Terms {
		promptVersion := term.PromptVersion
		if promptVersion == "" {
			promptVersion = defaultTermPromptVersion
		}
		metadata, err := json.Marshal(term.Metadata)
		if err != nil {
			return UpsertTranslationTermsResult{}, errors.Wrap(err, "marshal brreg translation term metadata")
		}
		if len(metadata) == 0 || string(metadata) == "null" {
			metadata = []byte(jsonPayloadEmptyObject)
		}
		err = queries.UpsertBrregTranslationTermResult(ctx, db.UpsertBrregTranslationTermResultParams{
			SourceLang:           term.SourceLang,
			TargetLang:           term.TargetLang,
			SourceTextNormalized: term.SourceTextNormalized,
			SourceText:           term.SourceText,
			TermKey:              term.TermKey,
			TranslatedText:       nilIfEmpty(term.TranslatedText),
			Status:               term.Status,
			Provider:             nilIfEmpty(term.Provider),
			Model:                nilIfEmpty(term.Model),
			PromptVersion:        promptVersion,
			Error:                nilIfEmpty(term.Error),
			ErrorCode:            nilIfEmpty(term.ErrorCode),
			Metadata:             metadata,
		})
		if err != nil {
			return UpsertTranslationTermsResult{}, errors.Wrap(err, "upsert brreg translation term result")
		}
	}
	return UpsertTranslationTermsResult{TermsUpserted: int32(len(command.Terms))}, nil
}

func (g *Gateway) ApplyCachedTermTranslations(
	ctx context.Context,
	command ApplyCachedTermTranslationsCommand,
) (ApplyCachedTermTranslationsResult, error) {
	if g.pool == nil {
		return ApplyCachedTermTranslationsResult{}, errors.New("brreg workflow database pool not available")
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultTermPromptVersion
	}
	queries := db.New(g.pool)
	companyFields, err := queries.ApplyBrregSourceCompanyCachedTranslationTerms(ctx, db.ApplyBrregSourceCompanyCachedTranslationTermsParams{
		PromptVersion: command.PromptVersion,
		Limit:         nonNegativeLimit(command.Limit),
	})
	if err != nil {
		return ApplyCachedTermTranslationsResult{}, errors.Wrap(err, "apply brreg company cached translation terms")
	}
	capitalFields, err := queries.ApplyBrregSourceCapitalCachedTranslationTerms(ctx, db.ApplyBrregSourceCapitalCachedTranslationTermsParams{
		PromptVersion: command.PromptVersion,
		Limit:         nonNegativeLimit(command.Limit),
	})
	if err != nil {
		return ApplyCachedTermTranslationsResult{}, errors.Wrap(err, "apply brreg capital cached translation terms")
	}
	return ApplyCachedTermTranslationsResult{FieldsApplied: companyFields + capitalFields}, nil
}

func (g *Gateway) CountMissingTranslationFields(ctx context.Context) (CountMissingTranslationFieldsResult, error) {
	if g.pool == nil {
		return CountMissingTranslationFieldsResult{}, errors.New("brreg workflow database pool not available")
	}
	missingFields, err := db.New(g.pool).CountBrregMissingTranslationFields(ctx)
	if err != nil {
		return CountMissingTranslationFieldsResult{}, errors.Wrap(err, "count brreg missing translation fields")
	}
	return CountMissingTranslationFieldsResult{MissingFields: missingFields}, nil
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func nonNegativeLimit(value int32) int32 {
	if value < 0 {
		return 0
	}
	return value
}

func positiveTermClaimLimit(value int32) int32 {
	if value <= 0 {
		return defaultTermClaimLimit
	}
	return value
}
