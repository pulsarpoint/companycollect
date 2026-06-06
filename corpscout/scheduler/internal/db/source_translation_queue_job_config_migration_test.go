package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceTranslationQueueJobConfigMigrationAddsDispatchMetadata(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000104_source_translation_queue_job_config.up.sql")
	require.NoError(t, err)
	sql := string(body)

	for _, schema := range []string{"brreg_source", "ariregister_source"} {
		require.Contains(t, sql, "ALTER TABLE "+schema+".translation_queue_entries")
		require.Contains(t, sql, "ADD COLUMN provider text NOT NULL DEFAULT 'default'")
		require.Contains(t, sql, "ADD COLUMN model text NOT NULL DEFAULT ''")
		require.Contains(t, sql, "ADD COLUMN prompt_version text NOT NULL DEFAULT 'v1'")
		require.Contains(t, sql, "ADD COLUMN source_lang text NOT NULL")
		require.Contains(t, sql, "ADD COLUMN target_lang text NOT NULL DEFAULT 'en'")
	}
}
