package actions

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

const termTranslationResultApplyLimit int32 = 10000

type TermTranslationResultConsumer struct {
	gateway *brregdb.Gateway
	subject string
}

func NewTermTranslationResultConsumer(
	gateway *brregdb.Gateway,
	_ *nats.Conn,
	subject string,
) *TermTranslationResultConsumer {
	if subject == "" {
		subject = translationclient.DefaultTermTranslationResultSubject
	}
	return &TermTranslationResultConsumer{
		gateway: gateway,
		subject: subject,
	}
}

func termResultsFromTranslationResult(result translationclient.TermTranslationResult) []brregdb.TranslationTermResult {
	sourceLang := itemOrDefault(result.SourceLang, "no")
	targetLang := itemOrDefault(result.TargetLang, "en")
	promptVersion := itemOrDefault(result.PromptVersion, "v1")
	terms := make([]brregdb.TranslationTermResult, 0, len(result.Results)+len(result.Failures))

	for _, item := range result.Results {
		terms = append(terms, brregdb.TranslationTermResult{
			SourceLang:           sourceLang,
			TargetLang:           targetLang,
			SourceTextNormalized: item.SourceTextNormalized,
			SourceText:           item.SourceText,
			TermKey:              item.TermKey,
			TranslatedText:       item.TranslatedText,
			Status:               item.Status,
			Provider:             result.Provider,
			Model:                result.Model,
			PromptVersion:        promptVersion,
		})
	}
	for _, failure := range result.Failures {
		terms = append(terms, brregdb.TranslationTermResult{
			SourceLang:           sourceLang,
			TargetLang:           targetLang,
			SourceTextNormalized: failure.SourceTextNormalized,
			SourceText:           failure.SourceText,
			TermKey:              failure.TermKey,
			Status:               failure.Status,
			Provider:             result.Provider,
			Model:                result.Model,
			PromptVersion:        promptVersion,
			Error:                failure.Error,
			ErrorCode:            failure.ErrorCode,
		})
	}
	return terms
}

func itemOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func (c *TermTranslationResultConsumer) HandleMessage(ctx context.Context, msg *nats.Msg) error {
	if c == nil || c.gateway == nil {
		return errors.New("brreg term translation result gateway not available")
	}
	if msg == nil {
		return errors.New("nats message not available")
	}

	var result translationclient.TermTranslationResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return errors.Wrap(err, "decode brreg term translation result")
	}

	terms := termResultsFromTranslationResult(result)
	if len(terms) == 0 {
		return errors.New("brreg term translation result contained no terms")
	}
	promptVersion := itemOrDefault(result.PromptVersion, "v1")
	upserted, err := c.gateway.UpsertTranslationTerms(ctx, brregdb.UpsertTranslationTermsCommand{Terms: terms})
	if err != nil {
		return errors.Wrap(err, "upsert brreg term translation results")
	}
	applied, err := c.gateway.ApplyCachedTermTranslations(ctx, brregdb.ApplyCachedTermTranslationsCommand{
		PromptVersion: promptVersion,
		Limit:         termTranslationResultApplyLimit,
	})
	if err != nil {
		return errors.Wrap(err, "apply brreg cached term translations after result")
	}

	slog.DebugContext(ctx, "handled brreg term translation result",
		"request_id", result.RequestID,
		"subject", itemOrDefault(c.subject, msg.Subject),
		"message_subject", msg.Subject,
		"provider", result.Provider,
		"model", result.Model,
		"prompt_version", promptVersion,
		"terms", len(terms),
		"terms_upserted", upserted.TermsUpserted,
		"fields_applied", applied.FieldsApplied,
	)
	return nil
}
