package sourcetranslation

import "strings"

// QueueTermBuildResult separates model work from cached company bindings.
type QueueTermBuildResult struct {
	UncachedTerms  []TranslationTerm
	CachedBindings []TranslationBinding
}

func BuildTranslationQueueTerms(
	fields []MissingField,
	cachedTerms map[string]CachedTerm,
) QueueTermBuildResult {
	result := QueueTermBuildResult{
		UncachedTerms:  make([]TranslationTerm, 0),
		CachedBindings: make([]TranslationBinding, 0),
	}
	seenUncachedTerms := make(map[string]struct{})
	for _, field := range fields {
		normalized := normalizeMissingFieldForQueue(field)
		if normalized.CompanyID == "" || normalized.SourceText == "" || normalized.TermKey == "" {
			continue
		}
		cached := cachedTerms[normalized.TermKey]
		cachedText := strings.TrimSpace(cached.TranslatedText)
		if cachedText != "" {
			result.CachedBindings = append(result.CachedBindings, TranslationBinding{
				CompanyID:      normalized.CompanyID,
				SourceTable:    normalized.SourceTable,
				SourceRowID:    normalized.SourceRowID,
				SourceColumn:   normalized.SourceColumn,
				TargetColumn:   normalized.TargetColumn,
				TranslatedText: cachedText,
			})
			continue
		}
		if _, ok := seenUncachedTerms[normalized.TermKey]; ok {
			continue
		}
		seenUncachedTerms[normalized.TermKey] = struct{}{}
		result.UncachedTerms = append(result.UncachedTerms, TranslationTerm{
			TermKey:              normalized.TermKey,
			SourceText:           normalized.SourceText,
			SourceTextNormalized: normalized.SourceTextNormalized,
		})
	}
	return result
}

func BuildTranslationBindingsForResults(
	fields []MissingField,
	results []TranslationTermResult,
) []TranslationBinding {
	succeededTerms := make(map[string]string, len(results))
	for _, result := range results {
		termKey := strings.TrimSpace(result.TermKey)
		translatedText := strings.TrimSpace(result.TranslatedText)
		if result.Status != "succeeded" || termKey == "" || translatedText == "" {
			continue
		}
		succeededTerms[termKey] = translatedText
	}
	bindings := make([]TranslationBinding, 0, len(fields))
	for _, field := range fields {
		normalized := normalizeMissingFieldForQueue(field)
		if normalized.CompanyID == "" || normalized.SourceText == "" || normalized.TermKey == "" {
			continue
		}
		translatedText := succeededTerms[normalized.TermKey]
		if translatedText == "" {
			continue
		}
		bindings = append(bindings, TranslationBinding{
			CompanyID:      normalized.CompanyID,
			SourceTable:    normalized.SourceTable,
			SourceRowID:    normalized.SourceRowID,
			SourceColumn:   normalized.SourceColumn,
			TargetColumn:   normalized.TargetColumn,
			TranslatedText: translatedText,
		})
	}
	return bindings
}

func TranslationTermKeys(fields []MissingField) []string {
	termKeys := make([]string, 0, len(fields))
	seenTermKeys := make(map[string]struct{})
	for _, field := range fields {
		normalized := normalizeMissingFieldForQueue(field)
		if normalized.CompanyID == "" || normalized.SourceText == "" || normalized.TermKey == "" {
			continue
		}
		if _, ok := seenTermKeys[normalized.TermKey]; ok {
			continue
		}
		seenTermKeys[normalized.TermKey] = struct{}{}
		termKeys = append(termKeys, normalized.TermKey)
	}
	return termKeys
}

func normalizeMissingFieldForQueue(field MissingField) MissingField {
	sourceText := strings.TrimSpace(field.SourceText)
	normalizedText := strings.TrimSpace(field.SourceTextNormalized)
	if normalizedText == "" {
		normalizedText = NormalizeText(sourceText)
	}
	termKey := strings.TrimSpace(field.TermKey)
	if termKey == "" && sourceText != "" {
		termKey = TermKey(sourceText)
	}
	return MissingField{
		CompanyID:            strings.TrimSpace(field.CompanyID),
		SourceTable:          strings.TrimSpace(field.SourceTable),
		SourceRowID:          strings.TrimSpace(field.SourceRowID),
		SourceColumn:         strings.TrimSpace(field.SourceColumn),
		TargetColumn:         strings.TrimSpace(field.TargetColumn),
		SourceText:           sourceText,
		SourceTextNormalized: normalizedText,
		TermKey:              termKey,
		Priority:             field.Priority,
	}
}
