CREATE TABLE brreg_workflow.task_selections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID NOT NULL REFERENCES brreg_workflow.workflow_runs(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  selection_hash TEXT NOT NULL UNIQUE,
  selection_definition JSONB NOT NULL DEFAULT '{}'::jsonb,
  records_selected INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_workflow_task_selection_type CHECK (
    task_type IN ('translate', 'discover_domains', 'convert_financials', 'build_enhanced', 'publish')
  ),
  CONSTRAINT chk_brreg_workflow_task_selection_count CHECK (records_selected >= 0),
  CONSTRAINT chk_brreg_workflow_task_selection_definition_object CHECK (jsonb_typeof(selection_definition) = 'object')
);

CREATE TABLE brreg_workflow.task_selection_records (
  selection_id UUID NOT NULL REFERENCES brreg_workflow.task_selections(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (selection_id, raw_record_id)
);

CREATE INDEX idx_brreg_workflow_task_selections_run
  ON brreg_workflow.task_selections (workflow_run_id, task_type, created_at DESC);

CREATE INDEX idx_brreg_workflow_task_selection_records_raw
  ON brreg_workflow.task_selection_records (raw_record_id, selection_id);
