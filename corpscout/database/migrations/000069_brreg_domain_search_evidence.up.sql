CREATE OR REPLACE VIEW brreg_workflow.v_domain_search_evidence AS
WITH action_search_artifacts AS (
  SELECT
    daa.id AS action_attempt_id,
    daa.workflow_run_id,
    daa.task_attempt_id,
    daa.raw_record_id,
    daa.provider AS search_engine,
    daa.model,
    daa.attempt,
    daa.status AS action_status,
    daa.started_at,
    daa.finished_at,
    daa.error AS action_error,
    daa.error_category,
    daa.error_code,
    daa.retry_strategy,
    (daa.metadata ->> 'search_term')::text AS search_term,
    daa.metadata AS action_metadata,
    daa_artifact.id AS artifact_id,
    daa_artifact.created_at AS artifact_created_at,
    daa_artifact.payload AS search_payload,
    daa_artifact.metadata AS artifact_metadata
  FROM brreg_workflow.domain_action_attempts daa
  JOIN LATERAL (
    SELECT artifact.*
    FROM brreg_workflow.domain_action_artifacts artifact
    WHERE artifact.attempt_id = daa.id
      AND artifact.artifact_type = 'search_page'
    ORDER BY artifact.created_at DESC, artifact.id DESC
    LIMIT 1
  ) daa_artifact ON true
  WHERE daa.action_type = 'search_page_fetch'
),
domain_result_search_artifacts AS (
  SELECT
    dr.id AS action_attempt_id,
    ta.workflow_run_id,
    dr.task_attempt_id,
    dr.raw_record_id,
    COALESCE(
      dr.domain_payload ->> 'search_engine',
      dr.domain_payload #>> '{search,metadata,search_engine}',
      dr.metadata ->> 'search_engine'
    )::text AS search_engine,
    (dr.metadata ->> 'model')::text AS model,
    COALESCE(ta.attempt, 1)::integer AS attempt,
    dr.status AS action_status,
    COALESCE(ta.started_at, dr.created_at) AS started_at,
    COALESCE(ta.finished_at, dr.created_at) AS finished_at,
    dr.error AS action_error,
    ta.error_category,
    ta.error_code,
    ta.retry_strategy,
    COALESCE(
      dr.domain_payload ->> 'search_term',
      dr.domain_payload #>> '{search,metadata,search_term}',
      dr.metadata ->> 'search_term',
      ''
    )::text AS search_term,
    dr.metadata AS action_metadata,
    dr.id AS artifact_id,
    dr.created_at AS artifact_created_at,
    dr.domain_payload -> 'search' AS search_payload,
    dr.metadata AS artifact_metadata
  FROM brreg_workflow.domain_results dr
  LEFT JOIN brreg_workflow.task_attempts ta
    ON ta.id = dr.task_attempt_id
  WHERE dr.domain_payload ? 'search'
    AND dr.domain_payload -> 'search' <> 'null'::jsonb
),
search_artifacts AS (
  SELECT * FROM action_search_artifacts
  UNION ALL
  SELECT * FROM domain_result_search_artifacts
)
SELECT
  action_attempt_id,
  workflow_run_id,
  task_attempt_id,
  raw_record_id,
  search_engine,
  model,
  attempt,
  action_status,
  started_at,
  finished_at,
  action_error,
  error_category,
  error_code,
  retry_strategy,
  COALESCE(search_term, '')::text AS search_term,
  action_metadata,
  artifact_id,
  artifact_created_at,
  COALESCE(search_payload ->> 'url', '')::text AS search_url,
  COALESCE(search_payload ->> 'final_url', '')::text AS final_url,
  COALESCE(search_payload ->> 'status', '')::text AS crawl_status,
  COALESCE(search_payload ->> 'markdown', '')::text AS markdown,
  COALESCE(search_payload ->> 'markdown_hash', '')::text AS markdown_hash,
  COALESCE(search_payload -> 'links', '[]'::jsonb)::jsonb AS links,
  COALESCE(search_payload -> 'metadata', '{}'::jsonb)::jsonb AS crawl_metadata,
  CASE
    WHEN search_payload ? 'error' AND jsonb_typeof(search_payload -> 'error') <> 'null'
      THEN search_payload -> 'error'
    ELSE '{}'::jsonb
  END AS crawl_error,
  COALESCE(search_payload, '{}'::jsonb)::jsonb AS artifact_payload,
  COALESCE(artifact_metadata, '{}'::jsonb)::jsonb AS artifact_metadata
FROM search_artifacts;

CREATE INDEX IF NOT EXISTS idx_brreg_workflow_domain_results_search_evidence
  ON brreg_workflow.domain_results(raw_record_id, created_at DESC)
  WHERE domain_payload ? 'search'
    AND domain_payload -> 'search' <> 'null'::jsonb;

GRANT SELECT ON brreg_workflow.v_domain_search_evidence TO corpscout_anon;
