package ariregisterdb

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultTermPromptVersion = "v1"

func (g *Gateway) UpsertTranslationTerms(
	ctx context.Context,
	command UpsertTranslationTermsCommand,
) (UpsertTranslationTermsResult, error) {
	if g == nil || g.pool == nil {
		return UpsertTranslationTermsResult{}, errors.New("ariregister workflow database pool not available")
	}
	queries := db.New(g.pool)
	for _, term := range command.Terms {
		promptVersion := term.PromptVersion
		if promptVersion == "" {
			promptVersion = defaultTermPromptVersion
		}
		metadata, err := json.Marshal(term.Metadata)
		if err != nil {
			return UpsertTranslationTermsResult{}, errors.Wrap(err, "marshal ariregister translation term metadata")
		}
		if len(metadata) == 0 || string(metadata) == "null" {
			metadata = []byte(jsonPayloadEmptyObject)
		}
		err = queries.UpsertAriregisterTranslationTermResult(ctx, db.UpsertAriregisterTranslationTermResultParams{
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
			return UpsertTranslationTermsResult{}, errors.Wrap(err, "upsert ariregister translation term result")
		}
	}
	return UpsertTranslationTermsResult{TermsUpserted: int32(len(command.Terms))}, nil
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
