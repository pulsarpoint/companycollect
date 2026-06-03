package actions

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

func TestTermResultsFromTranslationResultMapsSuccessAndFailure(t *testing.T) {
	result := translationclient.TermTranslationResult{
		RequestID:     "request-1",
		Source:        "brreg",
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "",
		Results: []translationclient.TermTranslationResultItem{{
			TermKey:              "term-success",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
		Failures: []translationclient.TermTranslationFailureResult{{
			TermKey:              "term-failure",
			SourceText:           "Ukjent",
			SourceTextNormalized: "ukjent",
			Status:               "failed",
			ErrorCode:            "provider_timeout",
			Error:                "provider timed out",
		}},
	}

	terms := termResultsFromTranslationResult(result)

	require.Equal(t, []brregdb.TranslationTermResult{
		{
			SourceLang:           "no",
			TargetLang:           "en",
			SourceTextNormalized: "aksjeselskap",
			SourceText:           "Aksjeselskap",
			TermKey:              "term-success",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
			Provider:             "mock",
			Model:                "mock-fast",
			PromptVersion:        "v1",
		},
		{
			SourceLang:           "no",
			TargetLang:           "en",
			SourceTextNormalized: "ukjent",
			SourceText:           "Ukjent",
			TermKey:              "term-failure",
			Status:               "failed",
			Provider:             "mock",
			Model:                "mock-fast",
			PromptVersion:        "v1",
			Error:                "provider timed out",
			ErrorCode:            "provider_timeout",
		},
	}, terms)
}

func TestTermTranslationResultConsumerRejectsEmptyResult(t *testing.T) {
	tx := testdb.BeginTx(t)
	consumer := NewTermTranslationResultConsumer(brregdb.New(tx), nil, "")

	err := consumer.HandleMessage(context.Background(), &nats.Msg{Data: []byte(`{}`)})

	require.Error(t, err)
	require.Contains(t, err.Error(), "contained no terms")
}
