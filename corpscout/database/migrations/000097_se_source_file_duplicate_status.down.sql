DROP INDEX IF EXISTS idx_se_workflow_source_files_hash_status;

UPDATE se_workflow.source_files
SET status = 'downloaded'
WHERE status = 'skipped_duplicate';

ALTER TABLE se_workflow.source_files
  DROP CONSTRAINT chk_se_workflow_source_file_status;

ALTER TABLE se_workflow.source_files
  ADD CONSTRAINT chk_se_workflow_source_file_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  );
