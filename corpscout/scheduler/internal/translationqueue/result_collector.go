package translationqueue

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type ResultCollector struct {
	registry SourceRegistry
}

func NewResultCollector(registry SourceRegistry) *ResultCollector {
	return &ResultCollector{registry: registry}
}

func (c *ResultCollector) HandleResult(ctx context.Context, result TranslationResult) error {
	if c == nil {
		return errors.New("translation result collector is required")
	}
	source, ok := c.registry.Get(result.Source)
	if !ok {
		return errors.Newf("translation result source %q is not registered", result.Source)
	}
	if strings.TrimSpace(result.BatchID) == "" {
		return errors.New("translation result batch id is required")
	}

	terms := translationTermsFromResult(result)
	if len(terms) == 0 {
		if shouldReleaseResultBatch(result) {
			_, err := source.ReleaseBatch(ctx, result.BatchID)
			return errors.Wrap(err, "release failed translation batch")
		}
		return errors.New("translation result terms are required")
	}

	if _, err := source.SaveTerms(ctx, sourcetranslation.SaveTermsCommand{
		PromptVersion: result.PromptVersion,
		Terms:         terms,
	}); err != nil {
		return errors.Wrap(err, "save translation result terms")
	}

	if len(result.Results) > 0 {
		companyIDs := resultCompanyIDs(result)
		if len(companyIDs) == 0 {
			return errors.New("translation result company ids are required")
		}
		fields, err := source.LoadMissingFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
			PromptVersion: result.PromptVersion,
			CompanyIDs:    companyIDs,
		})
		if err != nil {
			return errors.Wrap(err, "load missing fields for translation result")
		}
		bindings := sourcetranslation.BuildTranslationBindingsForResults(fields, terms)
		if len(bindings) > 0 {
			if _, err := applyTranslationBindingsByCompany(ctx, source, bindings); err != nil {
				return errors.Wrap(err, "apply translation result bindings")
			}
		}
	}

	if shouldReleaseResultBatch(result) {
		_, err := source.ReleaseBatch(ctx, result.BatchID)
		return errors.Wrap(err, "release incomplete translation result batch")
	}
	_, err := source.CompleteBatch(ctx, result.BatchID)
	return errors.Wrap(err, "complete translation result batch")
}

func translationTermsFromResult(result TranslationResult) []sourcetranslation.TranslationTermResult {
	terms := make([]sourcetranslation.TranslationTermResult, 0, len(result.Results)+len(result.Failures))
	for _, item := range result.Results {
		terms = append(terms, sourcetranslation.TranslationTermResult{
			TermKey:              item.TermKey,
			SourceText:           item.SourceText,
			SourceTextNormalized: item.SourceTextNormalized,
			TranslatedText:       item.TranslatedText,
			Status:               resultTermStatus(item.Status, "succeeded"),
			Provider:             result.Provider,
			Model:                result.Model,
			PromptVersion:        result.PromptVersion,
		})
	}
	for _, failure := range result.Failures {
		terms = append(terms, sourcetranslation.TranslationTermResult{
			TermKey:              failure.TermKey,
			SourceText:           failure.SourceText,
			SourceTextNormalized: failure.SourceTextNormalized,
			Status:               resultTermStatus(failure.Status, "failed_retryable"),
			Provider:             result.Provider,
			Model:                result.Model,
			PromptVersion:        result.PromptVersion,
			Error:                failure.Error,
			ErrorCode:            failure.ErrorCode,
		})
	}
	return terms
}

func shouldReleaseResultBatch(result TranslationResult) bool {
	status := strings.TrimSpace(result.Status)
	return status == "failed" || status == "partial" || len(result.Failures) > 0
}

func resultTermStatus(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func resultCompanyIDs(result TranslationResult) []string {
	companyIDs := make([]string, 0, len(result.CompanyIDs))
	seen := make(map[string]struct{})
	for _, companyID := range result.CompanyIDs {
		companyID = strings.TrimSpace(companyID)
		if companyID == "" {
			continue
		}
		if _, exists := seen[companyID]; exists {
			continue
		}
		seen[companyID] = struct{}{}
		companyIDs = append(companyIDs, companyID)
	}
	return companyIDs
}
