CREATE TABLE nace_source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  revision TEXT NOT NULL,
  source_url TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  content_length_bytes BIGINT NOT NULL,
  content_type TEXT,
  etag TEXT,
  last_modified TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  processed_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_source_files_revision CHECK (btrim(revision) <> ''),
  CONSTRAINT chk_nace_source_files_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_nace_source_files_sha256 CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_nace_source_files_content_length CHECK (content_length_bytes >= 0),
  CONSTRAINT chk_nace_source_files_status CHECK (status IN ('downloaded', 'processing', 'processed', 'failed')),
  CONSTRAINT chk_nace_source_files_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (revision, source_url, content_sha256)
);

CREATE TABLE nace_import_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  temporal_workflow_id TEXT NOT NULL UNIQUE,
  source_file_id UUID REFERENCES nace_source_files(id) ON DELETE SET NULL,
  revision TEXT NOT NULL,
  source_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  content_sha256 TEXT,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_imported INTEGER NOT NULL DEFAULT 0,
  records_deactivated INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_nace_import_runs_revision CHECK (btrim(revision) <> ''),
  CONSTRAINT chk_nace_import_runs_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_nace_import_runs_status CHECK (status IN ('running', 'skipped', 'succeeded', 'failed')),
  CONSTRAINT chk_nace_import_runs_sha256 CHECK (content_sha256 IS NULL OR content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_nace_import_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_nace_source_files_revision_status
  ON nace_source_files(revision, status, created_at DESC);

CREATE INDEX idx_nace_source_files_sha256
  ON nace_source_files(content_sha256);

CREATE INDEX idx_nace_import_runs_revision_started
  ON nace_import_runs(revision, started_at DESC);

CREATE OR REPLACE VIEW v_nace_source_file_imports AS
SELECT
  run.id AS import_run_id,
  run.temporal_workflow_id,
  run.revision,
  run.source_url,
  run.status AS import_status,
  run.content_sha256,
  run.records_seen,
  run.records_imported,
  run.records_deactivated,
  run.started_at,
  run.finished_at,
  CASE
    WHEN run.error IS NULL THEN NULL
    ELSE 'nace taxonomy import failed'
  END AS import_error,
  file.id AS source_file_id,
  file.status AS source_file_status,
  file.content_length_bytes,
  file.content_type,
  file.etag,
  file.last_modified,
  file.processed_at
FROM nace_import_runs run
LEFT JOIN nace_source_files file ON file.id = run.source_file_id;

GRANT SELECT ON nace_source_files TO corpscout_anon;
GRANT SELECT ON v_nace_source_file_imports TO corpscout_anon;
