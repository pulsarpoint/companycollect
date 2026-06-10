DELETE FROM data_source_action_runs
WHERE action = 'map_industries_to_nace';

DELETE FROM data_source_actions
WHERE action = 'map_industries_to_nace';

ALTER TABLE data_source_actions
  DROP CONSTRAINT IF EXISTS chk_data_source_actions_action;

ALTER TABLE data_source_action_runs
  DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action;

ALTER TABLE data_source_actions
  ADD CONSTRAINT chk_data_source_actions_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache')
  );

ALTER TABLE data_source_action_runs
  ADD CONSTRAINT chk_data_source_action_runs_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache')
  );
