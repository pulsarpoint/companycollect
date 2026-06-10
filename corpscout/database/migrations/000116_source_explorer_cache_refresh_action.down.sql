DELETE FROM data_source_action_runs
WHERE action = 'refresh_explorer_cache';

DELETE FROM data_source_actions
WHERE action = 'refresh_explorer_cache';

ALTER TABLE data_source_actions
  DROP CONSTRAINT IF EXISTS chk_data_source_actions_action;

ALTER TABLE data_source_action_runs
  DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action;

ALTER TABLE data_source_actions
  ADD CONSTRAINT chk_data_source_actions_action CHECK (
    action IN ('pull_source', 'import_clickhouse')
  );

ALTER TABLE data_source_action_runs
  ADD CONSTRAINT chk_data_source_action_runs_action CHECK (
    action IN ('pull_source', 'import_clickhouse')
  );
