package actions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslateAriregisterTranslationWorksetBatchRequiresSourceLanguage(t *testing.T) {
	actions := NewCompanyTranslationActions(nil, nil)

	_, err := actions.TranslateAriregisterTranslationWorksetBatch(context.Background(), TranslateAriregisterTranslationWorksetBatchInput{
		CompanyIDs: []string{"company-a"},
		TargetLang: "en",
		Provider:   "default",
		BatchID:    "batch-1",
	})

	require.ErrorContains(t, err, "ariregister translation queue batch source_lang is required")
}

func TestTranslateAriregisterTranslationWorksetBatchRequiresTargetLanguage(t *testing.T) {
	actions := NewCompanyTranslationActions(nil, nil)

	_, err := actions.TranslateAriregisterTranslationWorksetBatch(context.Background(), TranslateAriregisterTranslationWorksetBatchInput{
		CompanyIDs: []string{"company-a"},
		SourceLang: "et",
		Provider:   "default",
		BatchID:    "batch-1",
	})

	require.ErrorContains(t, err, "ariregister translation queue batch target_lang is required")
}
