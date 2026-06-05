ALTER TABLE se_workflow.source_files
  DROP CONSTRAINT chk_se_workflow_source_file_status;

ALTER TABLE se_workflow.source_files
  ADD CONSTRAINT chk_se_workflow_source_file_status CHECK (
    status IN ('downloaded', 'parsed', 'failed', 'skipped_duplicate')
  );

CREATE INDEX idx_se_workflow_source_files_hash_status
  ON se_workflow.source_files (dataset_key, payload_hash, status)
  WHERE payload_hash IS NOT NULL;
