package actions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslateBrregTranslationWorksetBatchRequiresSourceLanguage(t *testing.T) {
	actions := NewCompanyTranslationActions(nil, nil)

	_, err := actions.TranslateBrregTranslationWorksetBatch(context.Background(), TranslateBrregTranslationWorksetBatchInput{
		CompanyIDs: []string{"company-a"},
		TargetLang: "en",
		Provider:   "default",
		BatchID:    "batch-1",
	})

	require.ErrorContains(t, err, "brreg translation queue batch source_lang is required")
}

func TestTranslateBrregTranslationWorksetBatchRequiresTargetLanguage(t *testing.T) {
	actions := NewCompanyTranslationActions(nil, nil)

	_, err := actions.TranslateBrregTranslationWorksetBatch(context.Background(), TranslateBrregTranslationWorksetBatchInput{
		CompanyIDs: []string{"company-a"},
		SourceLang: "no",
		Provider:   "default",
		BatchID:    "batch-1",
	})

	require.ErrorContains(t, err, "brreg translation queue batch target_lang is required")
}
