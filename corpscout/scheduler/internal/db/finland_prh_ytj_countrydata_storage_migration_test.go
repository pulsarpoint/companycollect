package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinlandPRHYTJCountrydataStorageMigrationDefinesSourceSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000105_finland_prh_ytj_countrydata_storage.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS countrydata_finland_prh_ytj")
	require.Contains(t, sql, "CREATE TABLE countrydata_finland_prh_ytj.sources")
	require.Contains(t, sql, "CREATE TABLE countrydata_finland_prh_ytj.download_runs")
	require.Contains(t, sql, "CREATE TABLE countrydata_finland_prh_ytj.raw_records")
	require.Contains(t, sql, "supports_incremental BOOLEAN NOT NULL DEFAULT false")
	require.Contains(t, sql, "last_success_at TIMESTAMPTZ")
	require.Contains(t, sql, "snapshot_sha256 TEXT")
	require.Contains(t, sql, "duration_ms BIGINT")
	require.Contains(t, sql, "bytes_downloaded BIGINT")
	require.Contains(t, sql, "business_id TEXT NOT NULL")
	require.Contains(t, sql, "legal_name TEXT")
	require.Contains(t, sql, "raw_payload JSONB NOT NULL")
	require.Contains(t, sql, "payload_hash TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (business_id, payload_hash)")
	require.Contains(t, sql, "idx_countrydata_finland_prh_ytj_raw_records_current_business_id")
	require.Contains(t, sql, "'finland_prh_ytj_v3'")
	require.Contains(t, sql, "'countrydata_finland_prh_ytj.raw_records'")
	require.Contains(t, sql, "https://avoindata.prh.fi/opendata-ytj-api/v3/companies")
}

func TestFinlandPRHYTJCountrydataStorageDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000105_finland_prh_ytj_countrydata_storage.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DELETE FROM data_sources WHERE name = 'finland_prh_ytj_v3'")
	require.Contains(t, sql, "DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE")
}
