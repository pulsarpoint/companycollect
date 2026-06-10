ALTER TABLE data_sources
  ADD COLUMN IF NOT EXISTS source_url TEXT,
  ADD COLUMN IF NOT EXISTS docs_url TEXT,
  ADD COLUMN IF NOT EXISTS raw_source_retention TEXT,
  ADD COLUMN IF NOT EXISTS source_file_name TEXT,
  ADD COLUMN IF NOT EXISTS user_agent_required BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_file_name CHECK (
    source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json')
  );
