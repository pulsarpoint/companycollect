DROP VIEW IF EXISTS v_temporal_schedule_metadata;
DROP TRIGGER IF EXISTS trg_temporal_schedule_metadata_updated_at ON temporal_schedule_metadata;
DROP FUNCTION IF EXISTS set_temporal_schedule_metadata_updated_at();
DROP TABLE IF EXISTS temporal_schedule_metadata;
