package brregdb

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestUpsertTranslationTermsPersistsSucceededAndFailedTerms(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	result, err := New(tx).UpsertTranslationTerms(ctx, UpsertTranslationTermsCommand{
		Terms: []TranslationTermResult{
			{
				SourceLang:           "no",
				TargetLang:           "en",
				PromptVersion:        "v1",
				TermKey:              termKey("Aksjeselskap"),
				SourceTextNormalized: "aksjeselskap",
				SourceText:           "Aksjeselskap",
				TranslatedText:       "Limited liability company",
				Status:               "succeeded",
				Provider:             "deepseek",
				Model:                "deepseek-v4-flash",
			},
			{
				SourceLang:           "no",
				TargetLang:           "en",
				PromptVersion:        "v1",
				TermKey:              termKey("Enhet"),
				SourceTextNormalized: "enhet",
				SourceText:           "Enhet",
				Status:               "failed_retryable",
				Error:                "temporary failure",
				ErrorCode:            "llm_timeout",
			},
		},
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.TermsUpserted)

	var succeededCount int
	err = tx.QueryRow(ctx, `
SELECT count(*)
FROM brreg_source.translation_terms
WHERE status = 'succeeded'
  AND translated_text = 'Limited liability company'
  AND provider = 'deepseek'
  AND model = 'deepseek-v4-flash'
`).Scan(&succeededCount)
	require.NoError(t, err)
	require.Equal(t, 1, succeededCount)

	var failedCount int
	err = tx.QueryRow(ctx, `
SELECT count(*)
FROM brreg_source.translation_terms
WHERE status = 'failed_retryable'
  AND error = 'temporary failure'
  AND error_code = 'llm_timeout'
`).Scan(&failedCount)
	require.NoError(t, err)
	require.Equal(t, 1, failedCount)
}

func termKey(sourceText string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(sourceText))))
	return hex.EncodeToString(sum[:])
}
