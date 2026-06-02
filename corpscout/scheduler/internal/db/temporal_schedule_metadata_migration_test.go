package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemporalScheduleMetadataMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000073_temporal_schedule_metadata.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE temporal_schedule_metadata")
	require.Contains(t, sql, "temporal_schedule_id TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (temporal_schedule_id)")
	require.Contains(t, sql, "workflow_key TEXT NOT NULL")
	require.Contains(t, sql, "workflow_name TEXT NOT NULL")
	require.Contains(t, sql, "task_queue TEXT NOT NULL")
	require.Contains(t, sql, "metadata JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "jsonb_typeof(metadata) = 'object'")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_temporal_schedule_metadata")
	require.Contains(t, sql, "GRANT SELECT ON v_temporal_schedule_metadata TO corpscout_anon")
}

func TestTemporalScheduleMetadataDownMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000073_temporal_schedule_metadata.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_temporal_schedule_metadata")
	require.Contains(t, sql, "DROP TABLE IF EXISTS temporal_schedule_metadata")
}
