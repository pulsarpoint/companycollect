UPDATE data_source_actions
SET
  temporal_workflow_type = 'CompanySourcePullWorkflow',
  updated_at = now()
WHERE action = 'pull_source'
  AND temporal_workflow_type = 'CompanySourceDownloadWorkflow';
