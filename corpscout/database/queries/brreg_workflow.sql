-- name: BeginBrregWorkflowRun :one
INSERT INTO brreg_workflow.workflow_runs (
  orchestrator,
  orchestrator_run_id,
  run_type,
  metadata
) VALUES (
  COALESCE(sqlc.narg('orchestrator')::text, 'temporal'),
  sqlc.arg('orchestrator_run_id')::text,
  sqlc.arg('run_type')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (orchestrator_run_id) DO UPDATE
SET
  orchestrator = EXCLUDED.orchestrator,
  run_type = EXCLUDED.run_type,
  status = 'running',
  started_at = now(),
  finished_at = NULL,
  error = NULL,
  metadata = EXCLUDED.metadata
RETURNING id;

-- name: FinishBrregWorkflowRun :one
UPDATE brreg_workflow.workflow_runs
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  error = sqlc.narg('error')::text
WHERE id = sqlc.arg('id')::uuid
RETURNING id;

-- name: FinishBrregWorkflowRunWithStats :one
UPDATE brreg_workflow.workflow_runs
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  records_seen = sqlc.arg('records_seen')::integer,
  records_completed = sqlc.arg('records_completed')::integer,
  records_failed = sqlc.arg('records_failed')::integer,
  error = sqlc.narg('error')::text
WHERE id = sqlc.arg('id')::uuid
RETURNING id;

-- name: FailRunningBrregWorkflowTasksForRun :one
WITH params AS (
  SELECT
    sqlc.arg('max_attempts')::integer AS max_attempts,
    sqlc.narg('error')::text AS error,
    sqlc.arg('workflow_run_id')::uuid AS workflow_run_id
),
failed_attempts AS (
  UPDATE brreg_workflow.task_attempts ta
  SET
    status = 'failed',
    finished_at = now(),
    error = params.error,
    error_category = 'workflow_activity',
    error_code = 'activity_failed',
    retry_strategy = 'retry_with_backoff'
  FROM params
  WHERE ta.workflow_run_id = params.workflow_run_id
    AND ta.status = 'running'
  RETURNING ta.id, ta.raw_record_id, ta.task_type, ta.attempt
),
updated_task_states AS (
  UPDATE brreg_workflow.raw_record_task_states ts
  SET
    status = CASE
      WHEN failed_attempts.attempt >= params.max_attempts THEN 'failed_terminal'
      ELSE 'failed_retryable'
    END,
    attempt_count = GREATEST(ts.attempt_count, failed_attempts.attempt),
    last_attempt_id = failed_attempts.id,
    last_finished_at = now(),
    lease_until = NULL,
    next_retry_at = CASE
      WHEN failed_attempts.attempt >= params.max_attempts THEN NULL
      WHEN failed_attempts.attempt = 1 THEN now() + interval '5 minutes'
      WHEN failed_attempts.attempt = 2 THEN now() + interval '30 minutes'
      WHEN failed_attempts.attempt = 3 THEN now() + interval '6 hours'
      ELSE now() + interval '1 day'
    END,
    last_error = params.error,
    error_category = 'workflow_activity',
    error_code = 'activity_failed',
    retry_strategy = 'retry_with_backoff',
    updated_at = now()
  FROM failed_attempts
  CROSS JOIN params
  WHERE ts.raw_record_id = failed_attempts.raw_record_id
    AND ts.task_type = failed_attempts.task_type
    AND ts.last_attempt_id = failed_attempts.id
    AND ts.status = 'running'
  RETURNING ts.raw_record_id
)
SELECT count(*)::integer AS failed_tasks
FROM updated_task_states;

-- name: RecoverStaleBrregWorkflowRuns :one
WITH params AS (
  SELECT
    GREATEST(sqlc.arg('min_age_seconds')::integer, 1) AS min_age_seconds,
    GREATEST(sqlc.arg('max_attempts')::integer, 1) AS max_attempts
),
candidate_attempts AS (
  SELECT
    ta.id,
    ta.workflow_run_id,
    ta.raw_record_id,
    ta.task_type,
    ta.attempt
  FROM brreg_workflow.task_attempts ta
  JOIN brreg_workflow.workflow_runs wr ON wr.id = ta.workflow_run_id
  JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = ta.raw_record_id
   AND ts.task_type = ta.task_type
   AND ts.last_attempt_id = ta.id
  CROSS JOIN params
  WHERE wr.status = 'running'
    AND wr.started_at <= now() - make_interval(secs => params.min_age_seconds)
    AND ta.started_at <= now() - make_interval(secs => params.min_age_seconds)
    AND ta.status = 'running'
    AND ts.status = 'running'
    AND coalesce(ts.lease_until, ts.last_started_at + interval '30 minutes') <= now()
),
failed_attempts AS (
  UPDATE brreg_workflow.task_attempts ta
  SET
    status = 'failed',
    finished_at = now(),
    error = 'stale local workflow run recovered',
    error_category = 'workflow_activity',
    error_code = 'stale_workflow_recovered',
    retry_strategy = 'retry_with_backoff'
  FROM candidate_attempts ca
  WHERE ta.id = ca.id
  RETURNING ta.id, ta.workflow_run_id, ta.raw_record_id, ta.task_type, ta.attempt
),
updated_task_states AS (
  UPDATE brreg_workflow.raw_record_task_states ts
  SET
    status = CASE
      WHEN failed_attempts.attempt >= params.max_attempts THEN 'failed_terminal'
      ELSE 'failed_retryable'
    END,
    attempt_count = GREATEST(ts.attempt_count, failed_attempts.attempt),
    last_attempt_id = failed_attempts.id,
    last_finished_at = now(),
    lease_until = NULL,
    next_retry_at = CASE
      WHEN failed_attempts.attempt >= params.max_attempts THEN NULL
      WHEN failed_attempts.attempt = 1 THEN now() + interval '5 minutes'
      WHEN failed_attempts.attempt = 2 THEN now() + interval '30 minutes'
      WHEN failed_attempts.attempt = 3 THEN now() + interval '6 hours'
      ELSE now() + interval '1 day'
    END,
    last_error = 'stale local workflow run recovered',
    error_category = 'workflow_activity',
    error_code = 'stale_workflow_recovered',
    retry_strategy = 'retry_with_backoff',
    updated_at = now()
  FROM failed_attempts
  CROSS JOIN params
  WHERE ts.raw_record_id = failed_attempts.raw_record_id
    AND ts.task_type = failed_attempts.task_type
    AND ts.last_attempt_id = failed_attempts.id
    AND ts.status = 'running'
  RETURNING failed_attempts.workflow_run_id, ts.raw_record_id
),
per_run AS (
  SELECT workflow_run_id, count(*)::integer AS failed_tasks
  FROM updated_task_states
  GROUP BY workflow_run_id
),
audit_only_stale_runs AS (
  SELECT wr.id AS workflow_run_id, 0::integer AS failed_tasks
  FROM brreg_workflow.workflow_runs wr
  CROSS JOIN params
  WHERE wr.status = 'running'
    AND wr.started_at <= now() - make_interval(secs => params.min_age_seconds)
    AND NOT EXISTS (
      SELECT 1
      FROM brreg_workflow.task_attempts ta
      WHERE ta.workflow_run_id = wr.id
        AND ta.status = 'running'
    )
),
run_updates AS (
  SELECT workflow_run_id, failed_tasks FROM per_run
  UNION
  SELECT workflow_run_id, failed_tasks FROM audit_only_stale_runs
),
updated_runs AS (
  UPDATE brreg_workflow.workflow_runs wr
  SET
    status = 'failed',
    finished_at = now(),
    records_seen = wr.records_seen + run_updates.failed_tasks,
    records_failed = wr.records_failed + run_updates.failed_tasks,
    error = 'stale local workflow run recovered'
  FROM run_updates
  WHERE wr.id = run_updates.workflow_run_id
    AND wr.status = 'running'
  RETURNING wr.id
)
SELECT
  (SELECT count(*)::integer FROM updated_runs) AS workflow_runs_recovered,
  (SELECT count(*)::integer FROM updated_task_states) AS task_attempts_recovered;

-- name: GetCurrentBrregWorkflowRawRecord :one
SELECT id, payload_hash
FROM brreg_workflow.raw_records
WHERE organization_number = sqlc.arg('organization_number')::text
  AND is_current = true;

-- name: CountBrregWorkflowRawRecords :one
SELECT count(*)::bigint
FROM brreg_workflow.v_raw_record_list ri
WHERE (
    sqlc.narg('query')::text IS NULL
    OR ri.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR ri.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (sqlc.narg('lifecycle_state')::text IS NULL OR ri.lifecycle_state = sqlc.narg('lifecycle_state')::text)
  AND (sqlc.narg('translation_status')::text IS NULL OR ri.translation_status = sqlc.narg('translation_status')::text)
  AND (sqlc.narg('domain_status')::text IS NULL OR ri.domain_status = sqlc.narg('domain_status')::text)
  AND (sqlc.narg('financial_status')::text IS NULL OR ri.financial_status = sqlc.narg('financial_status')::text)
  AND (sqlc.narg('enhanced_status')::text IS NULL OR ri.enhanced_status = sqlc.narg('enhanced_status')::text)
  AND (
    sqlc.narg('domain_search')::text IS NULL
    OR (
      sqlc.narg('domain_search')::text = 'performed'
      AND EXISTS (
        SELECT 1
        FROM brreg_workflow.v_domain_search_evidence evidence
        WHERE evidence.raw_record_id = ri.id
      )
    )
    OR (
      sqlc.narg('domain_search')::text = 'with_markdown'
      AND EXISTS (
        SELECT 1
        FROM brreg_workflow.v_domain_search_evidence evidence
        WHERE evidence.raw_record_id = ri.id
          AND (
            (evidence.markdown IS NOT NULL AND evidence.markdown <> '')
            OR COALESCE(evidence.crawl_metadata ->> 'markdown_s3_key', '') <> ''
          )
      )
    )
    OR (
      sqlc.narg('domain_search')::text = 'missing'
      AND NOT EXISTS (
        SELECT 1
        FROM brreg_workflow.v_domain_search_evidence evidence
        WHERE evidence.raw_record_id = ri.id
      )
    )
  );

-- name: ListBrregWorkflowRawRecords :many
SELECT *
FROM brreg_workflow.v_raw_record_list ri
WHERE (
    sqlc.narg('query')::text IS NULL
    OR ri.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR ri.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (sqlc.narg('lifecycle_state')::text IS NULL OR ri.lifecycle_state = sqlc.narg('lifecycle_state')::text)
  AND (sqlc.narg('translation_status')::text IS NULL OR ri.translation_status = sqlc.narg('translation_status')::text)
  AND (sqlc.narg('domain_status')::text IS NULL OR ri.domain_status = sqlc.narg('domain_status')::text)
  AND (sqlc.narg('financial_status')::text IS NULL OR ri.financial_status = sqlc.narg('financial_status')::text)
  AND (sqlc.narg('enhanced_status')::text IS NULL OR ri.enhanced_status = sqlc.narg('enhanced_status')::text)
  AND (
    sqlc.narg('domain_search')::text IS NULL
    OR (
      sqlc.narg('domain_search')::text = 'performed'
      AND EXISTS (
        SELECT 1
        FROM brreg_workflow.v_domain_search_evidence evidence
        WHERE evidence.raw_record_id = ri.id
      )
    )
    OR (
      sqlc.narg('domain_search')::text = 'with_markdown'
      AND EXISTS (
        SELECT 1
        FROM brreg_workflow.v_domain_search_evidence evidence
        WHERE evidence.raw_record_id = ri.id
          AND (
            (evidence.markdown IS NOT NULL AND evidence.markdown <> '')
            OR COALESCE(evidence.crawl_metadata ->> 'markdown_s3_key', '') <> ''
          )
      )
    )
    OR (
      sqlc.narg('domain_search')::text = 'missing'
      AND NOT EXISTS (
        SELECT 1
        FROM brreg_workflow.v_domain_search_evidence evidence
        WHERE evidence.raw_record_id = ri.id
      )
    )
  )
ORDER BY
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'asc' THEN lower(COALESCE(ri.organization_name, '')) END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'desc' THEN lower(COALESCE(ri.organization_name, '')) END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'website' AND sqlc.arg('sort_dir')::text = 'asc' THEN lower(COALESCE(ri.website, '')) END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'website' AND sqlc.arg('sort_dir')::text = 'desc' THEN lower(COALESCE(ri.website, '')) END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'state' AND sqlc.arg('sort_dir')::text = 'asc' THEN ri.lifecycle_state END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'state' AND sqlc.arg('sort_dir')::text = 'desc' THEN ri.lifecycle_state END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'translation_status' AND sqlc.arg('sort_dir')::text = 'asc' THEN ri.translation_status END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'translation_status' AND sqlc.arg('sort_dir')::text = 'desc' THEN ri.translation_status END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'domain_status' AND sqlc.arg('sort_dir')::text = 'asc' THEN ri.domain_status END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'domain_status' AND sqlc.arg('sort_dir')::text = 'desc' THEN ri.domain_status END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'financial_status' AND sqlc.arg('sort_dir')::text = 'asc' THEN ri.financial_status END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'financial_status' AND sqlc.arg('sort_dir')::text = 'desc' THEN ri.financial_status END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'enhanced_status' AND sqlc.arg('sort_dir')::text = 'asc' THEN ri.enhanced_status END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'enhanced_status' AND sqlc.arg('sort_dir')::text = 'desc' THEN ri.enhanced_status END DESC,
  CASE WHEN sqlc.arg('sort_by')::text = 'last_seen_at' AND sqlc.arg('sort_dir')::text = 'asc' THEN ri.last_seen_at END ASC,
  CASE WHEN sqlc.arg('sort_by')::text = 'last_seen_at' AND sqlc.arg('sort_dir')::text = 'desc' THEN ri.last_seen_at END DESC,
  ri.last_seen_at DESC,
  ri.id ASC
LIMIT sqlc.arg('limit')::integer
OFFSET sqlc.arg('offset')::integer;

-- name: GetBrregWorkflowRawRecordDetail :one
SELECT *
FROM brreg_workflow.v_raw_record_detail
WHERE id = sqlc.arg('id')::uuid;

-- name: ListBrregWorkflowDomainSearchEvidenceByRawRecord :many
SELECT *
FROM brreg_workflow.v_domain_search_evidence
WHERE raw_record_id = sqlc.arg('raw_record_id')::uuid
ORDER BY started_at DESC, artifact_created_at DESC NULLS LAST, action_attempt_id DESC;

-- name: UpsertBrregWorkflowNACEMappingsForRawRecord :many
WITH source_sections AS (
  SELECT
    raw.id AS raw_record_id,
    source_section.source_field,
    source_section.classification_type,
    source_section.position,
    source_section.raw_section
  FROM brreg_workflow.raw_records raw
  CROSS JOIN LATERAL (
    SELECT
      'naeringskode1'::text AS source_field,
      'industry'::text AS classification_type,
      1::smallint AS position,
      raw.raw_payload -> 'naeringskode1' AS raw_section
    UNION ALL
    SELECT 'naeringskode2'::text, 'industry'::text, 2::smallint, raw.raw_payload -> 'naeringskode2'
    UNION ALL
    SELECT 'naeringskode3'::text, 'industry'::text, 3::smallint, raw.raw_payload -> 'naeringskode3'
    UNION ALL
    SELECT 'hjelpeenhetskode'::text, 'helper_unit'::text, 1::smallint, raw.raw_payload -> 'hjelpeenhetskode'
  ) AS source_section
  WHERE raw.id = sqlc.arg('raw_record_id')::uuid
    AND jsonb_typeof(source_section.raw_section) = 'object'
    AND COALESCE(source_section.raw_section ->> 'kode', '') <> ''
),
source_codes AS (
  SELECT
    source_sections.raw_record_id,
    source_sections.source_field,
    source_sections.classification_type,
    source_sections.position,
    source_sections.raw_section ->> 'kode' AS source_code,
    source_sections.raw_section ->> 'beskrivelse' AS source_description,
    regexp_replace(upper(source_sections.raw_section ->> 'kode'), '[^0-9A-Z]', '', 'g') AS normalized_source_code,
    source_sections.raw_section
  FROM source_sections
),
mapped_codes AS (
  SELECT
    source_codes.*,
    CASE
      WHEN source_codes.normalized_source_code ~ '^[0-9]{5}$'
        THEN substring(source_codes.normalized_source_code from 1 for 2) || '.' || substring(source_codes.normalized_source_code from 3 for 2)
      WHEN source_codes.normalized_source_code ~ '^[0-9]{4}$'
        THEN substring(source_codes.normalized_source_code from 1 for 2) || '.' || substring(source_codes.normalized_source_code from 3 for 2)
      ELSE NULL
    END AS mapped_nace_code,
    CASE
      WHEN source_codes.normalized_source_code ~ '^[0-9]{5}$' THEN 'sn_level_5_to_nace_class'
      WHEN source_codes.normalized_source_code ~ '^[0-9]{4}$' THEN 'nace_exact'
      ELSE NULL
    END AS mapping_method
  FROM source_codes
),
resolved_mappings AS (
  SELECT
    mapped_codes.raw_record_id,
    nace_code.id AS nace_code_id,
    mapped_codes.source_field,
    mapped_codes.classification_type,
    mapped_codes.position,
    mapped_codes.source_code,
    mapped_codes.source_description,
    mapped_codes.mapped_nace_code,
    mapped_codes.mapping_method,
    1::real AS confidence,
    jsonb_build_object(
      'source', 'brreg',
      'raw_section', mapped_codes.raw_section,
      'nace_revision', sqlc.arg('nace_revision')::text,
      'normalized_source_code', mapped_codes.normalized_source_code
    ) AS evidence
  FROM mapped_codes
  JOIN nace_classifications nace_classification
    ON nace_classification.code_system = 'NACE'
   AND nace_classification.revision = sqlc.arg('nace_revision')::text
  JOIN nace_codes nace_code
    ON nace_code.classification_id = nace_classification.id
   AND nace_code.code = mapped_codes.mapped_nace_code
   AND nace_code.level_name = 'class'
   AND nace_code.active
  WHERE mapped_codes.mapped_nace_code IS NOT NULL
    AND mapped_codes.mapping_method IS NOT NULL
),
upserted AS (
  INSERT INTO brreg_workflow.nace_mappings (
    raw_record_id,
    nace_code_id,
    source_field,
    classification_type,
    position,
    source_code,
    source_description,
    mapped_nace_code,
    mapping_method,
    confidence,
    evidence
  )
  SELECT
    raw_record_id,
    nace_code_id,
    source_field,
    classification_type,
    position,
    source_code,
    source_description,
    mapped_nace_code,
    mapping_method,
    confidence,
    evidence
  FROM resolved_mappings
  ON CONFLICT (raw_record_id, source_field, source_code, nace_code_id)
  DO UPDATE SET
    classification_type = EXCLUDED.classification_type,
    position = EXCLUDED.position,
    source_description = EXCLUDED.source_description,
    mapped_nace_code = EXCLUDED.mapped_nace_code,
    mapping_method = EXCLUDED.mapping_method,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING *
)
SELECT * FROM upserted
ORDER BY position, source_field;

-- name: ListBrregWorkflowNACEMappingsByRawRecord :many
SELECT *
FROM brreg_workflow.v_nace_mappings
WHERE raw_record_id = sqlc.arg('raw_record_id')::uuid
ORDER BY position, source_field, mapped_nace_code;

-- name: SupersedeCurrentBrregWorkflowRawRecord :exec
UPDATE brreg_workflow.raw_records
SET
  is_current = false,
  last_seen_at = now()
WHERE organization_number = sqlc.arg('organization_number')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertBrregWorkflowRawRecord :one
WITH existing_current AS (
  SELECT payload_hash
  FROM brreg_workflow.raw_records
  WHERE organization_number = sqlc.arg('organization_number')::text
    AND is_current = true
),
superseded_current AS (
  UPDATE brreg_workflow.raw_records
  SET
    is_current = false,
    last_seen_at = now()
  WHERE organization_number = sqlc.arg('organization_number')::text
    AND payload_hash <> sqlc.arg('payload_hash')::text
    AND is_current = true
  RETURNING id
),
upserted AS (
  INSERT INTO brreg_workflow.raw_records (
    bulk_snapshot_id,
    source_native_id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    source_updated_at,
    raw_payload,
    payload_hash,
    is_current,
    metadata
  ) VALUES (
    sqlc.narg('bulk_snapshot_id')::uuid,
    sqlc.arg('source_native_id')::text,
    sqlc.arg('organization_number')::text,
    sqlc.narg('organization_name')::text,
    sqlc.narg('registration_status')::text,
    sqlc.narg('website')::text,
    COALESCE(sqlc.narg('country_iso2')::text, 'NO'),
    sqlc.narg('source_updated_at')::timestamptz,
    sqlc.arg('raw_payload')::jsonb,
    sqlc.arg('payload_hash')::text,
    true,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (organization_number, payload_hash) DO UPDATE
  SET
    bulk_snapshot_id = EXCLUDED.bulk_snapshot_id,
    organization_name = EXCLUDED.organization_name,
    registration_status = EXCLUDED.registration_status,
    website = EXCLUDED.website,
    country_iso2 = EXCLUDED.country_iso2,
    source_updated_at = EXCLUDED.source_updated_at,
    raw_payload = EXCLUDED.raw_payload,
    is_current = true,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata
  RETURNING id
)
SELECT
  (SELECT id FROM upserted LIMIT 1) AS id,
  (SELECT count(*)::integer FROM upserted) AS rows_written,
  CASE WHEN NOT EXISTS (SELECT 1 FROM existing_current) THEN 1 ELSE 0 END::integer AS rows_inserted_new,
  CASE
    WHEN EXISTS (SELECT 1 FROM existing_current WHERE payload_hash = sqlc.arg('payload_hash')::text) THEN 1
    ELSE 0
  END::integer AS rows_existing_unchanged,
  CASE
    WHEN EXISTS (SELECT 1 FROM existing_current WHERE payload_hash <> sqlc.arg('payload_hash')::text) THEN 1
    ELSE 0
  END::integer AS rows_new_versions;

-- name: CreateBrregWorkflowTaskSelection :one
WITH filtered_records AS (
  SELECT
    rr.id,
    rr.last_seen_at,
    ts.raw_record_id AS task_state_raw_record_id,
    ts.status AS task_status,
    ts.attempt_count,
    ts.next_retry_at,
    ts.lease_until,
    ts.last_started_at,
    COALESCE(cardinality(sqlc.arg('selected_ids')::text[]), 0) > 0 AS selected_by_id,
    (
      (sqlc.arg('task_type')::text = 'translate' AND ri.translation_status IN ('not_started', 'failed'))
      OR (sqlc.arg('task_type')::text = 'discover_domains' AND ri.domain_status IN ('not_started', 'failed'))
      OR (sqlc.arg('task_type')::text = 'convert_financials' AND ri.financial_status IN ('not_started', 'failed'))
    ) AS artifact_needed
  FROM brreg_workflow.v_raw_record_list ri
  JOIN brreg_workflow.raw_records rr ON rr.id = ri.id
  LEFT JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = rr.id
   AND ts.task_type = sqlc.arg('task_type')::text
  WHERE rr.is_current = true
    AND (
      COALESCE(cardinality(sqlc.arg('selected_ids')::text[]), 0) = 0
      OR rr.id::text = ANY(sqlc.arg('selected_ids')::text[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR ri.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR ri.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (
      sqlc.narg('lifecycle_state')::text IS NULL
      OR ri.lifecycle_state = sqlc.narg('lifecycle_state')::text
    )
    AND (
      sqlc.narg('translation_status')::text IS NULL
      OR ri.translation_status = sqlc.narg('translation_status')::text
    )
    AND (
      sqlc.narg('domain_status')::text IS NULL
      OR ri.domain_status = sqlc.narg('domain_status')::text
    )
    AND (
      sqlc.narg('financial_status')::text IS NULL
      OR ri.financial_status = sqlc.narg('financial_status')::text
    )
    AND (
      sqlc.narg('enhanced_status')::text IS NULL
      OR ri.enhanced_status = sqlc.narg('enhanced_status')::text
    )
),
eligible_records AS (
  SELECT id
  FROM filtered_records
  WHERE (
    (task_state_raw_record_id IS NULL AND artifact_needed)
    OR task_status = 'pending'
    OR (
      selected_by_id
      AND artifact_needed
      AND task_status IN ('failed_retryable', 'failed_terminal')
    )
    OR (
      task_status = 'failed_retryable'
      AND attempt_count < sqlc.arg('max_attempts')::integer
      AND (next_retry_at IS NULL OR next_retry_at <= now())
    )
    OR (
      task_status = 'running'
      AND attempt_count < sqlc.arg('max_attempts')::integer
      AND COALESCE(lease_until, last_started_at + interval '30 minutes') <= now()
    )
  )
  ORDER BY last_seen_at ASC, id ASC
  LIMIT sqlc.arg('limit')::integer
),
selection AS (
  INSERT INTO brreg_workflow.task_selections (
    workflow_run_id,
    task_type,
    selection_hash,
    selection_definition,
    records_selected
  ) VALUES (
    sqlc.arg('workflow_run_id')::uuid,
    sqlc.arg('task_type')::text,
    sqlc.arg('selection_hash')::text,
    COALESCE(sqlc.narg('selection_definition')::jsonb, '{}'::jsonb),
    (SELECT count(*)::integer FROM eligible_records)
  )
  RETURNING id, selection_hash, records_selected
),
inserted_records AS (
  INSERT INTO brreg_workflow.task_selection_records (
    selection_id,
    raw_record_id
  )
  SELECT selection.id, eligible_records.id
  FROM selection
  CROSS JOIN eligible_records
  ON CONFLICT (selection_id, raw_record_id) DO NOTHING
  RETURNING raw_record_id
)
SELECT id, selection_hash, records_selected
FROM selection;

-- name: ClaimBrregWorkflowTaskSelectionBatch :many
WITH lock_task AS (
  SELECT pg_advisory_xact_lock(hashtext('brreg_workflow.raw_record_task_states:' || sqlc.arg('task_type')::text))
),
active_slots AS (
  SELECT GREATEST(sqlc.arg('max_parallel_tasks')::integer - count(*)::integer, 0) AS available_slots
  FROM brreg_workflow.raw_record_task_states ts
  CROSS JOIN lock_task
  WHERE ts.task_type = sqlc.arg('task_type')::text
    AND ts.status = 'running'
    AND COALESCE(ts.lease_until, ts.last_started_at + interval '30 minutes') > now()
),
selected_raw_records AS (
  SELECT
    rr.id,
    rr.last_seen_at,
    COALESCE(jsonb_array_length(
      CASE
        WHEN jsonb_typeof(s.selection_definition->'ids') = 'array' THEN s.selection_definition->'ids'
        ELSE '[]'::jsonb
      END
    ), 0) > 0 AS selected_by_id,
    (
      (sqlc.arg('task_type')::text = 'translate' AND ri.translation_status IN ('not_started', 'failed'))
      OR (sqlc.arg('task_type')::text = 'discover_domains' AND ri.domain_status IN ('not_started', 'failed'))
      OR (sqlc.arg('task_type')::text = 'convert_financials' AND ri.financial_status IN ('not_started', 'failed'))
    ) AS needs_artifact
  FROM brreg_workflow.task_selections s
  JOIN brreg_workflow.task_selection_records sr ON sr.selection_id = s.id
  JOIN brreg_workflow.raw_records rr ON rr.id = sr.raw_record_id
  JOIN brreg_workflow.v_raw_record_list ri ON ri.id = rr.id
  WHERE s.selection_hash = sqlc.arg('selection_hash')::text
    AND s.task_type = sqlc.arg('task_type')::text
    AND rr.is_current = true
),
new_task_ids AS (
  SELECT srr.id, srr.last_seen_at AS sort_at
  FROM selected_raw_records srr
  WHERE NOT EXISTS (
    SELECT 1
    FROM brreg_workflow.raw_record_task_states ts
    WHERE ts.raw_record_id = srr.id
      AND ts.task_type = sqlc.arg('task_type')::text
  )
    AND srr.needs_artifact
  ORDER BY srr.last_seen_at ASC, srr.id ASC
  LIMIT sqlc.arg('batch_size')::integer
),
pending_task_ids AS (
  SELECT ts.raw_record_id AS id, COALESCE(ts.next_retry_at, srr.last_seen_at) AS sort_at
  FROM selected_raw_records srr
  JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = srr.id
   AND ts.task_type = sqlc.arg('task_type')::text
  WHERE ts.status = 'pending'
  ORDER BY ts.next_retry_at ASC NULLS FIRST, ts.raw_record_id ASC
  LIMIT sqlc.arg('batch_size')::integer
),
failed_task_ids AS (
  SELECT ts.raw_record_id AS id, COALESCE(ts.next_retry_at, srr.last_seen_at) AS sort_at
  FROM selected_raw_records srr
  JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = srr.id
   AND ts.task_type = sqlc.arg('task_type')::text
  WHERE srr.needs_artifact
    AND (
      (
        srr.selected_by_id
        AND ts.status IN ('failed_retryable', 'failed_terminal')
      )
      OR (
        ts.status = 'failed_retryable'
        AND ts.attempt_count < sqlc.arg('max_attempts')::integer
        AND (ts.next_retry_at IS NULL OR ts.next_retry_at <= now())
      )
    )
  ORDER BY ts.next_retry_at ASC NULLS FIRST, ts.raw_record_id ASC
  LIMIT sqlc.arg('batch_size')::integer
),
stale_running_task_ids AS (
  SELECT ts.raw_record_id AS id, ts.last_started_at AS sort_at
  FROM selected_raw_records srr
  JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = srr.id
   AND ts.task_type = sqlc.arg('task_type')::text
  WHERE ts.status = 'running'
    AND ts.attempt_count < sqlc.arg('max_attempts')::integer
    AND COALESCE(ts.lease_until, ts.last_started_at + interval '30 minutes') <= now()
  ORDER BY ts.last_started_at ASC, ts.raw_record_id ASC
  LIMIT sqlc.arg('batch_size')::integer
),
candidate_ids AS (
  SELECT id, sort_at FROM new_task_ids
  UNION ALL
  SELECT id, sort_at FROM pending_task_ids
  UNION ALL
  SELECT id, sort_at FROM failed_task_ids
  UNION ALL
  SELECT id, sort_at FROM stale_running_task_ids
  ORDER BY sort_at ASC NULLS FIRST, id ASC
  LIMIT (SELECT LEAST(sqlc.arg('batch_size')::integer, available_slots) FROM active_slots)
),
attempt_rows AS (
  SELECT
    candidate_ids.id,
    COALESCE(MAX(ta.attempt), 0) + 1 AS attempt
  FROM candidate_ids
  LEFT JOIN brreg_workflow.task_attempts ta
    ON ta.raw_record_id = candidate_ids.id
   AND ta.task_type = sqlc.arg('task_type')::text
  GROUP BY candidate_ids.id
),
attempts AS (
  INSERT INTO brreg_workflow.task_attempts (
    workflow_run_id,
    raw_record_id,
    task_type,
    attempt,
    status,
    worker_id,
    started_at,
    metadata
  )
  SELECT
    sqlc.arg('workflow_run_id')::uuid,
    attempt_rows.id,
    sqlc.arg('task_type')::text,
    attempt_rows.attempt,
    'running',
    sqlc.narg('worker_id')::text,
    now(),
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  FROM attempt_rows
  RETURNING id, raw_record_id, attempt
),
claimed_task_ids AS (
  INSERT INTO brreg_workflow.raw_record_task_states (
    raw_record_id,
    task_type,
    status,
    attempt_count,
    last_attempt_id,
    last_started_at,
    last_finished_at,
    next_retry_at,
    lease_until,
    last_error,
    error_category,
    error_code,
    retry_strategy,
    result_summary
  )
  SELECT
    attempts.raw_record_id,
    sqlc.arg('task_type')::text,
    'running',
    attempts.attempt,
    attempts.id,
    now(),
    NULL,
    NULL,
    now() + make_interval(secs => sqlc.arg('lease_seconds')::integer),
    NULL,
    NULL,
    NULL,
    NULL,
    '{}'::jsonb
  FROM attempts
  ON CONFLICT (raw_record_id, task_type) DO UPDATE
  SET
    status = 'running',
    attempt_count = GREATEST(brreg_workflow.raw_record_task_states.attempt_count, EXCLUDED.attempt_count),
    last_attempt_id = EXCLUDED.last_attempt_id,
    last_started_at = now(),
    last_finished_at = NULL,
    next_retry_at = NULL,
    lease_until = now() + make_interval(secs => sqlc.arg('lease_seconds')::integer),
    last_error = NULL,
    error_category = NULL,
    error_code = NULL,
    retry_strategy = NULL,
    updated_at = now()
  WHERE brreg_workflow.raw_record_task_states.task_type = sqlc.arg('task_type')::text
    AND (
      brreg_workflow.raw_record_task_states.status = 'pending'
      OR brreg_workflow.raw_record_task_states.status IN ('failed_retryable', 'failed_terminal')
      OR (
        brreg_workflow.raw_record_task_states.status = 'running'
        AND brreg_workflow.raw_record_task_states.attempt_count < sqlc.arg('max_attempts')::integer
        AND COALESCE(
          brreg_workflow.raw_record_task_states.lease_until,
          brreg_workflow.raw_record_task_states.last_started_at + interval '30 minutes'
        ) <= now()
      )
    )
  RETURNING raw_record_id AS id
)
SELECT
  rr.id AS raw_record_id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  rr.raw_payload,
  attempts.id AS task_attempt_id,
  attempts.attempt
FROM attempts
JOIN brreg_workflow.raw_records rr ON rr.id = attempts.raw_record_id
JOIN claimed_task_ids cti ON cti.id = attempts.raw_record_id
WHERE rr.is_current = true
ORDER BY rr.last_seen_at ASC, rr.id ASC;

-- name: FinishBrregWorkflowTaskAttempt :exec
WITH finished_attempt AS (
  UPDATE brreg_workflow.task_attempts
  SET
    status = sqlc.arg('status')::text,
    finished_at = now(),
    error = sqlc.narg('error')::text,
    error_category = sqlc.narg('error_category')::text,
    error_code = sqlc.narg('error_code')::text,
    retry_strategy = sqlc.narg('retry_strategy')::text
  WHERE id = sqlc.arg('task_attempt_id')::uuid
  RETURNING id, raw_record_id, task_type, attempt, status
),
deleted_completed_state AS (
  DELETE FROM brreg_workflow.raw_record_task_states ts
  USING finished_attempt fa
  WHERE ts.raw_record_id = fa.raw_record_id
    AND ts.task_type = fa.task_type
    AND fa.status IN ('succeeded', 'skipped')
  RETURNING ts.raw_record_id
)
UPDATE brreg_workflow.raw_record_task_states ts
SET
  status = CASE
    WHEN fa.status = 'failed'
      AND (
        fa.attempt >= sqlc.arg('max_attempts')::integer
        OR sqlc.narg('retry_strategy')::text IN ('change_model_or_prompt', 'manual_config', 'manual_input', 'not_retryable')
      ) THEN 'failed_terminal'
    WHEN fa.status = 'failed' THEN 'failed_retryable'
    WHEN fa.status = 'cancelled' THEN 'cancelled'
    ELSE ts.status
  END,
  attempt_count = GREATEST(ts.attempt_count, fa.attempt),
  last_attempt_id = fa.id,
  last_finished_at = now(),
  lease_until = NULL,
  next_retry_at = CASE
    WHEN fa.status <> 'failed'
      OR fa.attempt >= sqlc.arg('max_attempts')::integer
      OR sqlc.narg('retry_strategy')::text IN ('change_model_or_prompt', 'manual_config', 'manual_input', 'not_retryable') THEN NULL
    WHEN fa.attempt = 1 THEN now() + interval '5 minutes'
    WHEN fa.attempt = 2 THEN now() + interval '30 minutes'
    WHEN fa.attempt = 3 THEN now() + interval '6 hours'
    ELSE now() + interval '1 day'
  END,
  last_error = sqlc.narg('error')::text,
  error_category = sqlc.narg('error_category')::text,
  error_code = sqlc.narg('error_code')::text,
  retry_strategy = sqlc.narg('retry_strategy')::text,
  updated_at = now()
FROM finished_attempt fa
WHERE ts.raw_record_id = fa.raw_record_id
  AND ts.task_type = fa.task_type
  AND fa.status NOT IN ('succeeded', 'skipped');

-- name: InsertBrregWorkflowTranslationResult :exec
INSERT INTO brreg_workflow.translation_results (
  raw_record_id,
  task_attempt_id,
  status,
  translated_payload,
  model,
  prompt_version,
  error,
  metadata
) VALUES (
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('task_attempt_id')::uuid,
  sqlc.arg('status')::text,
  sqlc.narg('translated_payload')::jsonb,
  sqlc.narg('model')::text,
  sqlc.narg('prompt_version')::text,
  sqlc.narg('error')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
);

-- name: InsertBrregWorkflowDomainResult :exec
INSERT INTO brreg_workflow.domain_results (
  raw_record_id,
  task_attempt_id,
  status,
  best_domain,
  domain_payload,
  error,
  metadata
) VALUES (
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('task_attempt_id')::uuid,
  sqlc.arg('status')::text,
  sqlc.narg('best_domain')::text,
  COALESCE(sqlc.narg('domain_payload')::jsonb, '{}'::jsonb),
  sqlc.narg('error')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
);

-- name: InsertBrregWorkflowFinancialResult :exec
INSERT INTO brreg_workflow.financial_results (
  raw_record_id,
  task_attempt_id,
  status,
  original_currency,
  original_payload,
  usd_payload,
  fx_metadata,
  source_uri,
  error,
  metadata
) VALUES (
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('task_attempt_id')::uuid,
  sqlc.arg('status')::text,
  sqlc.narg('original_currency')::text,
  COALESCE(sqlc.narg('original_payload')::jsonb, '{}'::jsonb),
  COALESCE(sqlc.narg('usd_payload')::jsonb, '{}'::jsonb),
  COALESCE(sqlc.narg('fx_metadata')::jsonb, '{}'::jsonb),
  sqlc.narg('source_uri')::text,
  sqlc.narg('error')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
);

-- name: CreateBrregDomainActionAttempt :one
INSERT INTO brreg_workflow.domain_action_attempts (
  workflow_run_id,
  task_attempt_id,
  raw_record_id,
  action_type,
  provider,
  model,
  input_hash,
  attempt,
  metadata
) VALUES (
  sqlc.narg('workflow_run_id')::uuid,
  sqlc.narg('task_attempt_id')::uuid,
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('action_type')::text,
  sqlc.narg('provider')::text,
  sqlc.narg('model')::text,
  sqlc.arg('input_hash')::text,
  sqlc.arg('attempt')::integer,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
RETURNING id;

-- name: InsertBrregDomainActionArtifact :exec
INSERT INTO brreg_workflow.domain_action_artifacts (
  attempt_id,
  raw_record_id,
  artifact_type,
  payload,
  payload_hash,
  metadata
) VALUES (
  sqlc.arg('attempt_id')::uuid,
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('artifact_type')::text,
  COALESCE(sqlc.arg('payload')::jsonb, '{}'::jsonb),
  sqlc.arg('payload_hash')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
);

-- name: FinishBrregDomainActionAttempt :exec
UPDATE brreg_workflow.domain_action_attempts
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  error = sqlc.narg('error')::text,
  error_category = sqlc.narg('error_category')::text,
  error_code = sqlc.narg('error_code')::text,
  retry_strategy = sqlc.narg('retry_strategy')::text,
  metadata = COALESCE(sqlc.narg('metadata')::jsonb, metadata)
WHERE id = sqlc.arg('id')::uuid;

-- name: GetBrregWorkflowTranslationAssetState :one
SELECT * FROM brreg_workflow.v_translation_asset_state;

-- name: GetBrregWorkflowDomainAssetState :one
SELECT * FROM brreg_workflow.v_domain_asset_state;

-- name: GetBrregWorkflowFinancialAssetState :one
SELECT * FROM brreg_workflow.v_financial_asset_state;

-- name: GetBrregWorkflowEnhancedAssetState :one
SELECT * FROM brreg_workflow.v_enhanced_asset_state;

-- name: ListBrregWorkflowEnhancedReadyRecords :many
SELECT *
FROM brreg_workflow.v_enhanced_ready_records
ORDER BY raw_last_seen_at ASC, id ASC;
