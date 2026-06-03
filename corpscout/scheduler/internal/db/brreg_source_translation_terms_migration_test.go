package db_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestBRREGSourceTranslationTermsMigration(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_source.translation_terms (
  source,
  source_lang,
  target_lang,
  source_text_normalized,
  source_text,
  term_key,
  status
) VALUES (
  'brreg',
  'no',
  'en',
  'aksjeselskap',
  'Aksjeselskap',
  repeat('a', 64),
  'pending'
)`)
	require.NoError(t, err)

	var uniqueIndexExists bool
	err = tx.QueryRow(ctx, `
SELECT to_regclass('brreg_source.uq_brreg_source_translation_terms_key') IS NOT NULL
`).Scan(&uniqueIndexExists)
	require.NoError(t, err)
	require.True(t, uniqueIndexExists)

	var statusIndexExists bool
	err = tx.QueryRow(ctx, `
SELECT to_regclass('brreg_source.idx_brreg_source_translation_terms_status') IS NOT NULL
`).Scan(&statusIndexExists)
	require.NoError(t, err)
	require.True(t, statusIndexExists)

	var lookupIndexExists bool
	err = tx.QueryRow(ctx, `
SELECT to_regclass('brreg_source.idx_brreg_source_translation_terms_lookup') IS NOT NULL
`).Scan(&lookupIndexExists)
	require.NoError(t, err)
	require.True(t, lookupIndexExists)

	var viewExists bool
	err = tx.QueryRow(ctx, `
SELECT to_regclass('brreg_source.v_missing_translation_fields') IS NOT NULL
`).Scan(&viewExists)
	require.NoError(t, err)
	require.True(t, viewExists)
}

func TestBRREGSourceTermTranslationStatsUseLatestTermStatus(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000080_brreg_source_term_translation_stats.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "latest_term_status AS")
	require.Contains(t, sql, "SELECT DISTINCT ON (source_lang, target_lang, term_key)")
	require.Contains(t, sql, "ORDER BY source_lang, target_lang, term_key, updated_at DESC, created_at DESC, id DESC")
	require.NotContains(t, sql, "bool_or(status = 'succeeded')")
}
