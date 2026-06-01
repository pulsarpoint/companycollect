CREATE TABLE brreg_workflow.domain_action_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES brreg_workflow.workflow_runs(id) ON DELETE SET NULL,
  task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL,
  provider TEXT,
  model TEXT,
  input_hash TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_domain_action_type CHECK (
    action_type IN (
      'existing_website_check',
      'search_page_fetch',
      'search_result_analysis',
      'candidate_site_crawl',
      'candidate_site_analysis',
      'domain_decision'
    )
  ),
  CONSTRAINT chk_brreg_domain_action_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_brreg_domain_action_attempt CHECK (attempt > 0),
  CONSTRAINT chk_brreg_domain_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX idx_brreg_domain_action_attempts_unique_attempt
  ON brreg_workflow.domain_action_attempts (
    raw_record_id,
    action_type,
    COALESCE(provider, ''),
    COALESCE(model, ''),
    input_hash,
    attempt
  );

CREATE INDEX idx_brreg_domain_action_attempts_raw_action
  ON brreg_workflow.domain_action_attempts(raw_record_id, action_type, started_at DESC);

CREATE INDEX idx_brreg_domain_action_attempts_status
  ON brreg_workflow.domain_action_attempts(status, action_type, started_at DESC);

CREATE INDEX idx_brreg_domain_action_attempts_task_attempt
  ON brreg_workflow.domain_action_attempts(task_attempt_id)
  WHERE task_attempt_id IS NOT NULL;

CREATE TABLE brreg_workflow.domain_action_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  attempt_id UUID NOT NULL REFERENCES brreg_workflow.domain_action_attempts(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_domain_action_artifact_type CHECK (
    artifact_type IN (
      'search_page',
      'search_candidates',
      'crawl_page',
      'site_analysis',
      'domain_decision'
    )
  ),
  CONSTRAINT chk_brreg_domain_action_artifact_payload_object CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT chk_brreg_domain_action_artifact_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (attempt_id, artifact_type, payload_hash)
);

CREATE INDEX idx_brreg_domain_action_artifacts_raw_type
  ON brreg_workflow.domain_action_artifacts(raw_record_id, artifact_type, created_at DESC);

CREATE INDEX idx_brreg_domain_action_artifacts_attempt
  ON brreg_workflow.domain_action_artifacts(attempt_id, created_at DESC);

GRANT SELECT ON brreg_workflow.domain_action_attempts TO corpscout_anon;
GRANT SELECT ON brreg_workflow.domain_action_artifacts TO corpscout_anon;
