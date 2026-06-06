package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareBrregTranslationQueueUsesMaterializedStatusEstimates(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_translation_queue.sql")
	require.NoError(t, err)

	query := querySection(
		string(body),
		"-- name: PrepareBrregTranslationQueue :one",
		"-- name: ResetStaleBrregTranslationQueueEntries :one",
	)

	require.Contains(t, query, "brreg_source.mv_company_translation_status")
	require.Contains(t, query, "translation_status.estimated_missing_chars")
	require.NotContains(t, query, "brreg_source.v_missing_translations")
}

func TestPrepareAriregisterTranslationQueueUsesMaterializedStatusEstimates(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/ariregister_translation_queue.sql")
	require.NoError(t, err)

	query := querySection(
		string(body),
		"-- name: PrepareAriregisterTranslationQueue :one",
		"-- name: ResetStaleAriregisterTranslationQueueEntries :one",
	)

	require.Contains(t, query, "ariregister_source.mv_company_translation_status")
	require.Contains(t, query, "translation_status.estimated_missing_chars")
	require.NotContains(t, query, "ariregister_source.v_missing_translations")
}

func TestTranslationQueuePreparePersistsDispatchConfig(t *testing.T) {
	files := []string{
		"../../../../database/queries/brreg_translation_queue.sql",
		"../../../../database/queries/ariregister_translation_queue.sql",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		require.NoError(t, err)
		sql := string(body)
		require.Contains(t, sql, "provider, model, prompt_version, source_lang, target_lang")
		require.Contains(t, sql, "sqlc.arg('provider')::text")
		require.Contains(t, sql, "sqlc.arg('model')::text")
		require.Contains(t, sql, "sqlc.arg('prompt_version')::text")
		require.Contains(t, sql, "sqlc.arg('source_lang')::text")
		require.Contains(t, sql, "sqlc.arg('target_lang')::text")
	}
}

func TestTranslationQueueClaimUsesHomogeneousDispatchConfig(t *testing.T) {
	files := []string{
		"../../../../database/queries/brreg_translation_queue.sql",
		"../../../../database/queries/ariregister_translation_queue.sql",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		require.NoError(t, err)
		sql := string(body)
		require.Contains(t, sql, "first_config AS")
		require.Contains(t, sql, "pending.provider = first_config.provider")
		require.Contains(t, sql, "pending.model = first_config.model")
		require.Contains(t, sql, "pending.prompt_version = first_config.prompt_version")
		require.Contains(t, sql, "RETURNING queue.company_id, queue.num_of_characters, queue.provider, queue.model, queue.prompt_version, queue.source_lang, queue.target_lang")
	}
}
