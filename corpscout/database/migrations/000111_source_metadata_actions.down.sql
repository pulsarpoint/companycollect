DROP TABLE IF EXISTS data_source_action_runs;
DROP TABLE IF EXISTS data_source_actions;

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_country_source,
  DROP CONSTRAINT IF EXISTS chk_data_sources_storage_kind,
  DROP CONSTRAINT IF EXISTS uq_data_sources_registry_key;

DROP INDEX IF EXISTS idx_data_sources_country_source;

ALTER TABLE data_sources
  DROP COLUMN IF EXISTS clickhouse_table_prefix,
  DROP COLUMN IF EXISTS clickhouse_database,
  DROP COLUMN IF EXISTS storage_kind,
  DROP COLUMN IF EXISTS auth_required,
  DROP COLUMN IF EXISTS registry_key,
  DROP COLUMN IF EXISTS source,
  DROP COLUMN IF EXISTS country;

ALTER TABLE data_sources
  ADD COLUMN IF NOT EXISTS schedule_enabled BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS schedule_kind TEXT NOT NULL DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS schedule_expression TEXT,
  ADD COLUMN IF NOT EXISTS last_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_failed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_source_marker_type TEXT,
  ADD COLUMN IF NOT EXISTS last_source_marker TEXT,
  ADD COLUMN IF NOT EXISTS last_source_modified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_error TEXT,
  ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS country_id UUID REFERENCES countries(id);

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_schedule_kind CHECK (
    schedule_kind IN ('manual', 'interval', 'cron', 'event')
  ),
  ADD CONSTRAINT chk_data_sources_marker_pair CHECK (
    (last_source_marker_type IS NULL AND last_source_marker IS NULL)
    OR (last_source_marker_type IS NOT NULL AND last_source_marker IS NOT NULL)
  ),
  ADD CONSTRAINT chk_data_sources_failures CHECK (consecutive_failures >= 0);
