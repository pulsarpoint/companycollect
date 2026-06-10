ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_file_name,
  DROP COLUMN IF EXISTS user_agent_required,
  DROP COLUMN IF EXISTS source_file_name,
  DROP COLUMN IF EXISTS raw_source_retention,
  DROP COLUMN IF EXISTS docs_url,
  DROP COLUMN IF EXISTS source_url;
