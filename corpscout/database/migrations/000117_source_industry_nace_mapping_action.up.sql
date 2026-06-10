ALTER TABLE data_source_actions
  DROP CONSTRAINT IF EXISTS chk_data_source_actions_action;

ALTER TABLE data_source_action_runs
  DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action;

ALTER TABLE data_source_actions
  ADD CONSTRAINT chk_data_source_actions_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache', 'map_industries_to_nace')
  );

ALTER TABLE data_source_action_runs
  ADD CONSTRAINT chk_data_source_action_runs_action CHECK (
    action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache', 'map_industries_to_nace')
  );

INSERT INTO data_source_actions (
  source_id, action, display_name, temporal_workflow_type, temporal_task_queue
)
SELECT
  ds.id,
  'map_industries_to_nace',
  'Map industries to NACE',
  'CompanySourceIndustryNACEMappingWorkflow',
  'corpscout-company-sources'
FROM data_sources ds
WHERE ds.registry_key = 'finland/prhytj'
ON CONFLICT (source_id, action) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  temporal_workflow_type = EXCLUDED.temporal_workflow_type,
  temporal_task_queue = EXCLUDED.temporal_task_queue,
  enabled = true,
  updated_at = now();
