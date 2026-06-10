DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_statement_artifacts;
DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_discovery_windows;
DROP SCHEMA IF EXISTS financial_xbrl;

DELETE FROM data_source_action_runs
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_source_actions
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_source_file_runs
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_source_files
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_sources
WHERE registry_key = 'finland/prh_xbrl';

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_group;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_group CHECK (
    source_group IN (
      'security_identifier', 'registry', 'domain', 'website',
      'github', 'ai_research', 'manual', 'other'
    )
  );

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_file_name;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_file_name CHECK (
    source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json')
  );

ALTER TABLE data_source_files
  DROP CONSTRAINT IF EXISTS chk_data_source_files_kind;

ALTER TABLE data_source_files
  ADD CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')
  );
