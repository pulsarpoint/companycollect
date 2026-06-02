CREATE TABLE temporal_schedule_metadata (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  temporal_schedule_id TEXT NOT NULL,
  workflow_key TEXT NOT NULL,
  workflow_name TEXT NOT NULL,
  task_queue TEXT NOT NULL,
  domain TEXT NOT NULL,
  purpose TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT,
  enabled BOOLEAN NOT NULL DEFAULT true,
  tags TEXT[] NOT NULL DEFAULT '{}',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_temporal_schedule_metadata_schedule_id UNIQUE (temporal_schedule_id),
  CONSTRAINT chk_temporal_schedule_metadata_schedule_id CHECK (btrim(temporal_schedule_id) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_workflow_key CHECK (btrim(workflow_key) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_workflow_name CHECK (btrim(workflow_name) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_task_queue CHECK (btrim(task_queue) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_domain CHECK (btrim(domain) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_purpose CHECK (btrim(purpose) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_display_name CHECK (btrim(display_name) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_temporal_schedule_metadata_workflow_key
  ON temporal_schedule_metadata(workflow_key, enabled);

CREATE INDEX idx_temporal_schedule_metadata_domain_purpose
  ON temporal_schedule_metadata(domain, purpose);

CREATE INDEX idx_temporal_schedule_metadata_tags
  ON temporal_schedule_metadata USING gin(tags);

CREATE OR REPLACE FUNCTION set_temporal_schedule_metadata_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_temporal_schedule_metadata_updated_at
BEFORE UPDATE ON temporal_schedule_metadata
FOR EACH ROW
EXECUTE FUNCTION set_temporal_schedule_metadata_updated_at();

CREATE OR REPLACE VIEW v_temporal_schedule_metadata AS
SELECT
  id,
  temporal_schedule_id,
  workflow_key,
  workflow_name,
  task_queue,
  domain,
  purpose,
  display_name,
  description,
  enabled,
  tags,
  metadata,
  created_at,
  updated_at
FROM temporal_schedule_metadata;

GRANT SELECT ON v_temporal_schedule_metadata TO corpscout_anon;
