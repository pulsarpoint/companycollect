ALTER TABLE brreg_company_raw_inputs
  ADD COLUMN state TEXT NOT NULL DEFAULT 'input',
  ADD CONSTRAINT chk_brreg_raw_input_state CHECK (
    state IN ('input', 'suggestion_submitted', 'completed', 'superseded')
  );

CREATE TABLE brreg_raw_input_actions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL CHECK (action_type IN ('translate', 'enhance', 'submit_suggestion')),
  attempt INTEGER NOT NULL,
  payload_hash TEXT NOT NULL,
  trigger TEXT,
  worker_id TEXT,
  workflow_id TEXT,
  workflow_run_id TEXT,
  river_job_id BIGINT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (raw_input_id, action_type, attempt)
);

CREATE TABLE brreg_raw_input_action_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  action_id UUID NOT NULL REFERENCES brreg_raw_input_actions(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
  message TEXT,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_brreg_raw_input_actions_raw_type_attempt
  ON brreg_raw_input_actions (raw_input_id, action_type, attempt DESC, created_at DESC);
CREATE INDEX idx_brreg_raw_input_actions_payload
  ON brreg_raw_input_actions (payload_hash);
CREATE INDEX idx_brreg_raw_input_action_events_latest
  ON brreg_raw_input_action_events (action_id, created_at DESC);
CREATE INDEX idx_brreg_raw_input_action_events_status
  ON brreg_raw_input_action_events (status, created_at);

UPDATE brreg_company_raw_inputs bri
SET state = CASE
  WHEN EXISTS (
    SELECT 1
    FROM suggestions s
    WHERE s.source_input_table = 'brreg_company_raw_inputs'
      AND s.source_input_id = bri.id::text
  ) THEN 'suggestion_submitted'
  WHEN bri.processing_status = 'superseded' THEN 'superseded'
  WHEN bri.processing_status IN ('processed', 'ignored') THEN 'completed'
  ELSE 'input'
END,
updated_at = now();

INSERT INTO brreg_raw_input_actions (
  raw_input_id,
  action_type,
  attempt,
  payload_hash,
  trigger,
  worker_id,
  metadata,
  created_at
)
SELECT
  bri.id,
  'translate',
  1,
  bri.payload_hash,
  'migration',
  bri.translation_lease_by,
  jsonb_build_object('legacy_translation_status', bri.translation_status),
  COALESCE(bri.translated_at, bri.updated_at, bri.created_at)
FROM brreg_company_raw_inputs bri
WHERE bri.translation_status IN ('translated', 'failed');

INSERT INTO brreg_raw_input_action_events (action_id, status, message, error, created_at)
SELECT
  a.id,
  CASE bri.translation_status WHEN 'translated' THEN 'succeeded' ELSE 'failed' END,
  'backfilled from legacy translation_status',
  bri.translation_error,
  COALESCE(bri.translated_at, bri.updated_at, bri.created_at)
FROM brreg_raw_input_actions a
JOIN brreg_company_raw_inputs bri ON bri.id = a.raw_input_id
WHERE a.action_type = 'translate'
  AND a.trigger = 'migration';

INSERT INTO brreg_raw_input_actions (
  raw_input_id,
  action_type,
  attempt,
  payload_hash,
  trigger,
  worker_id,
  metadata,
  created_at
)
SELECT
  bri.id,
  'submit_suggestion',
  1,
  bri.payload_hash,
  'migration',
  bri.processing_lease_by,
  jsonb_build_object('legacy_processing_status', bri.processing_status),
  COALESCE(bri.processed_at, bri.updated_at, bri.created_at)
FROM brreg_company_raw_inputs bri
WHERE bri.processing_status IN ('processed', 'failed')
   OR EXISTS (
     SELECT 1
     FROM suggestions s
     WHERE s.source_input_table = 'brreg_company_raw_inputs'
       AND s.source_input_id = bri.id::text
   );

INSERT INTO brreg_raw_input_action_events (action_id, status, message, error, created_at)
SELECT
  a.id,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM suggestions s
      WHERE s.source_input_table = 'brreg_company_raw_inputs'
        AND s.source_input_id = bri.id::text
    ) THEN 'succeeded'
    WHEN bri.processing_status = 'failed' THEN 'failed'
    ELSE 'skipped'
  END,
  'backfilled from legacy processing_status',
  bri.processing_error,
  COALESCE(bri.processed_at, bri.updated_at, bri.created_at)
FROM brreg_raw_input_actions a
JOIN brreg_company_raw_inputs bri ON bri.id = a.raw_input_id
WHERE a.action_type = 'submit_suggestion'
  AND a.trigger = 'migration';

CREATE OR REPLACE VIEW v_brreg_raw_input_action_attributes AS
WITH latest_actions AS (
  SELECT DISTINCT ON (a.raw_input_id, a.action_type)
    a.raw_input_id,
    a.action_type,
    a.id AS action_id
  FROM brreg_raw_input_actions a
  JOIN brreg_company_raw_inputs bri
    ON bri.id = a.raw_input_id
   AND bri.payload_hash = a.payload_hash
  ORDER BY a.raw_input_id, a.action_type, a.attempt DESC, a.created_at DESC
),
latest_action_statuses AS (
  SELECT
    la.raw_input_id,
    la.action_type,
    COALESCE(le.status, 'notdone') AS status
  FROM latest_actions la
  LEFT JOIN LATERAL (
    SELECT e.status
    FROM brreg_raw_input_action_events e
    WHERE e.action_id = la.action_id
    ORDER BY e.created_at DESC
    LIMIT 1
  ) le ON true
),
successful_actions AS (
  SELECT
    a.raw_input_id,
    a.action_type,
    bool_or(e.status = 'succeeded') AS has_success
  FROM brreg_raw_input_actions a
  JOIN brreg_company_raw_inputs bri
    ON bri.id = a.raw_input_id
   AND bri.payload_hash = a.payload_hash
  JOIN brreg_raw_input_action_events e ON e.action_id = a.id
  GROUP BY a.raw_input_id, a.action_type
)
SELECT
  bri.id AS raw_input_id,
  COALESCE(lt.status, 'notdone') AS latest_translation_action_status,
  COALESCE(st.has_success, false) AS has_successful_translation,
  COALESCE(le.status, 'notdone') AS latest_enhancement_action_status,
  COALESCE(se.has_success, false) AS has_successful_enhancement,
  COALESCE(ls.status, 'notdone') AS latest_submission_action_status,
  COALESCE(ss.has_success, false) AS has_successful_submission
FROM brreg_company_raw_inputs bri
LEFT JOIN latest_action_statuses lt
  ON lt.raw_input_id = bri.id
 AND lt.action_type = 'translate'
LEFT JOIN successful_actions st
  ON st.raw_input_id = bri.id
 AND st.action_type = 'translate'
LEFT JOIN latest_action_statuses le
  ON le.raw_input_id = bri.id
 AND le.action_type = 'enhance'
LEFT JOIN successful_actions se
  ON se.raw_input_id = bri.id
 AND se.action_type = 'enhance'
LEFT JOIN latest_action_statuses ls
  ON ls.raw_input_id = bri.id
 AND ls.action_type = 'submit_suggestion'
LEFT JOIN successful_actions ss
  ON ss.raw_input_id = bri.id
 AND ss.action_type = 'submit_suggestion';

CREATE INDEX idx_brreg_raw_inputs_state
  ON brreg_company_raw_inputs (state, created_at);
CREATE INDEX idx_brreg_raw_inputs_state_input
  ON brreg_company_raw_inputs (created_at)
  WHERE state = 'input';
CREATE INDEX idx_brreg_raw_inputs_state_submitted
  ON brreg_company_raw_inputs (created_at)
  WHERE state = 'suggestion_submitted';

CREATE OR REPLACE VIEW v_source_raw_inputs AS
  SELECT
    gri.id,
    'gleif' AS source_name,
    'gleif_company_raw_inputs' AS source_input_table,
    gri.lei AS source_native_id,
    gri.processing_status,
    gri.processing_attempts,
    gri.processing_error,
    gri.first_seen_at,
    gri.last_seen_at,
    gri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'gleif_company_raw_inputs'
        AND ssl.source_input_key = gri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission
  FROM gleif_company_raw_inputs gri

  UNION ALL

  SELECT
    chri.id,
    'companies_house' AS source_name,
    'companies_house_company_raw_inputs' AS source_input_table,
    chri.company_number AS source_native_id,
    chri.processing_status,
    chri.processing_attempts,
    chri.processing_error,
    chri.first_seen_at,
    chri.last_seen_at,
    chri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'companies_house_company_raw_inputs'
        AND ssl.source_input_key = chri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission
  FROM companies_house_company_raw_inputs chri

  UNION ALL

  SELECT
    bri.id,
    'brreg' AS source_name,
    'brreg_company_raw_inputs' AS source_input_table,
    bri.organization_number AS source_native_id,
    bri.processing_status,
    bri.processing_attempts,
    bri.processing_error,
    bri.first_seen_at,
    bri.last_seen_at,
    bri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestions s
      WHERE s.source_input_table = 'brreg_company_raw_inputs'
        AND s.source_input_id = bri.id::text
    ) AS has_suggestion,
    bri.raw_payload_en,
    bri.translation_status,
    bri.translation_attempts,
    bri.translation_error,
    bri.translation_model,
    bri.translation_prompt_version,
    bri.translated_at,
    bri.translation_fx_source,
    bri.translation_fx_rate_date,
    bri.state,
    COALESCE(baa.latest_translation_action_status, 'notdone') AS latest_translation_action_status,
    COALESCE(baa.has_successful_translation, false) AS has_successful_translation,
    COALESCE(baa.latest_enhancement_action_status, 'notdone') AS latest_enhancement_action_status,
    COALESCE(baa.has_successful_enhancement, false) AS has_successful_enhancement,
    COALESCE(baa.latest_submission_action_status, 'notdone') AS latest_submission_action_status,
    COALESCE(baa.has_successful_submission, false) AS has_successful_submission
  FROM brreg_company_raw_inputs bri
  LEFT JOIN v_brreg_raw_input_action_attributes baa ON baa.raw_input_id = bri.id

  UNION ALL

  SELECT
    cvri.id,
    'cvr' AS source_name,
    'cvr_company_raw_inputs' AS source_input_table,
    cvri.cvr_number AS source_native_id,
    cvri.processing_status,
    cvri.processing_attempts,
    cvri.processing_error,
    cvri.first_seen_at,
    cvri.last_seen_at,
    cvri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'cvr_company_raw_inputs'
        AND ssl.source_input_key = cvri.id::text
    ) AS has_suggestion,
    cvri.raw_payload_en,
    cvri.translation_status,
    cvri.translation_attempts,
    cvri.translation_error,
    cvri.translation_model,
    cvri.translation_prompt_version,
    cvri.translated_at,
    cvri.translation_fx_source,
    cvri.translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission
  FROM cvr_company_raw_inputs cvri

  UNION ALL

  SELECT
    ari.id,
    'ariregister' AS source_name,
    'ariregister_company_raw_inputs' AS source_input_table,
    ari.registry_code AS source_native_id,
    ari.processing_status,
    ari.processing_attempts,
    ari.processing_error,
    ari.first_seen_at,
    ari.last_seen_at,
    ari.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'ariregister_company_raw_inputs'
        AND ssl.source_input_key = ari.id::text
    ) AS has_suggestion,
    ari.raw_payload_en,
    ari.translation_status,
    ari.translation_attempts,
    ari.translation_error,
    ari.translation_model,
    ari.translation_prompt_version,
    ari.translated_at,
    ari.translation_fx_source,
    ari.translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission
  FROM ariregister_company_raw_inputs ari

  UNION ALL

  SELECT
    acpri.id,
    'ai_company_profile' AS source_name,
    'ai_company_profile_raw_inputs' AS source_input_table,
    COALESCE(acpri.normalized_domain, '') AS source_native_id,
    acpri.processing_status,
    acpri.processing_attempts,
    acpri.processing_error,
    acpri.first_seen_at,
    acpri.last_seen_at,
    acpri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'ai_company_profile_raw_inputs'
        AND ssl.source_input_key = acpri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission
  FROM ai_company_profile_raw_inputs acpri

  UNION ALL

  SELECT
    ddri.id,
    'domain_discovery' AS source_name,
    'domain_discovery_raw_inputs' AS source_input_table,
    ddri.domain AS source_native_id,
    ddri.processing_status,
    ddri.processing_attempts,
    ddri.processing_error,
    ddri.first_seen_at,
    ddri.last_seen_at,
    ddri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'domain_discovery_raw_inputs'
        AND ssl.source_input_key = ddri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission
  FROM domain_discovery_raw_inputs ddri;

GRANT SELECT ON v_brreg_raw_input_action_attributes TO corpscout_anon;
GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
