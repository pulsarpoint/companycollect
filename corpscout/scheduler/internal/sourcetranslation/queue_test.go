package sourcetranslation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTranslationQueueTermsSeparatesCachedBindingsAndUncachedTerms(t *testing.T) {
	computedKey := TermKey("  Äriühing  ")
	fields := []MissingField{
		{
			CompanyID:            " company-a ",
			SourceTable:          " companies ",
			SourceRowID:          " row-a ",
			SourceColumn:         " legal_form ",
			TargetColumn:         " legal_form_en ",
			SourceText:           " Osaühing ",
			TermKey:              " term-legal-form ",
			SourceTextNormalized: " osauhing ",
		},
		{
			CompanyID:    "company-b",
			SourceTable:  "companies",
			SourceRowID:  "row-b",
			SourceColumn: "legal_form",
			TargetColumn: "legal_form_en",
			SourceText:   "Osaühing",
			TermKey:      "term-legal-form",
		},
		{
			CompanyID:    "company-c",
			SourceTable:  "industries",
			SourceRowID:  "row-c",
			SourceColumn: "label",
			TargetColumn: "label_en",
			SourceText:   "Jaekaubandus",
			TermKey:      "term-retail",
		},
		{
			CompanyID:    "company-d",
			SourceTable:  "companies",
			SourceRowID:  "row-d",
			SourceColumn: "legal_form",
			TargetColumn: "legal_form_en",
			SourceText:   "  Äriühing  ",
		},
		{
			CompanyID:    "company-e",
			SourceTable:  "companies",
			SourceRowID:  "row-e",
			SourceColumn: "legal_form",
			TargetColumn: "legal_form_en",
			SourceText:   "   ",
			TermKey:      "blank-source-text",
		},
	}

	result := BuildTranslationQueueTerms(fields, map[string]CachedTerm{
		"term-retail": {TermKey: "term-retail", TranslatedText: " Retail trade "},
	})

	require.Equal(t, []TranslationTerm{
		{
			TermKey:              "term-legal-form",
			SourceText:           "Osaühing",
			SourceTextNormalized: "osauhing",
		},
		{
			TermKey:              computedKey,
			SourceText:           "Äriühing",
			SourceTextNormalized: NormalizeText("Äriühing"),
		},
	}, result.UncachedTerms)
	require.Equal(t, []TranslationBinding{{
		CompanyID:      "company-c",
		SourceTable:    "industries",
		SourceRowID:    "row-c",
		SourceColumn:   "label",
		TargetColumn:   "label_en",
		TranslatedText: "Retail trade",
	}}, result.CachedBindings)
}

func TestBuildTranslationBindingsForResultsUsesSucceededTermsOnly(t *testing.T) {
	fields := []MissingField{
		{
			CompanyID:    "company-a",
			SourceTable:  "companies",
			SourceRowID:  "row-a",
			SourceColumn: "legal_form",
			TargetColumn: "legal_form_en",
			SourceText:   "Osaühing",
			TermKey:      "term-legal-form",
		},
		{
			CompanyID:    "company-b",
			SourceTable:  "industries",
			SourceRowID:  "row-b",
			SourceColumn: "label",
			TargetColumn: "label_en",
			SourceText:   "Jaekaubandus",
			TermKey:      "term-retail",
		},
		{
			CompanyID:    "company-c",
			SourceTable:  "industries",
			SourceRowID:  "row-c",
			SourceColumn: "label",
			TargetColumn: "label_en",
			SourceText:   "Transport",
			TermKey:      "term-transport",
		},
	}

	bindings := BuildTranslationBindingsForResults(fields, []TranslationTermResult{
		{TermKey: "term-legal-form", Status: "succeeded", TranslatedText: " Private limited company "},
		{TermKey: "term-retail", Status: "failed_retryable", TranslatedText: "Retail trade"},
		{TermKey: "term-transport", Status: "succeeded"},
	})

	require.Equal(t, []TranslationBinding{{
		CompanyID:      "company-a",
		SourceTable:    "companies",
		SourceRowID:    "row-a",
		SourceColumn:   "legal_form",
		TargetColumn:   "legal_form_en",
		TranslatedText: "Private limited company",
	}}, bindings)
}
