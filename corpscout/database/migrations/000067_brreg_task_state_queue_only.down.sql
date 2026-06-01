ALTER TABLE brreg_workflow.raw_record_task_states
  DROP CONSTRAINT IF EXISTS chk_brreg_workflow_task_state_status;

ALTER TABLE brreg_workflow.raw_record_task_states
  ADD CONSTRAINT chk_brreg_workflow_task_state_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'skipped', 'failed_retryable', 'failed_terminal', 'cancelled')
  );
