-- name: GetSourceByName :one
SELECT
  id,
  name,
  country,
  source,
  registry_key,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  auth_required,
  storage_kind,
  clickhouse_database,
  clickhouse_table_prefix,
  COALESCE(source_url, '') AS source_url,
  COALESCE(docs_url, '') AS docs_url,
  COALESCE(raw_source_retention, '') AS raw_source_retention,
  COALESCE(source_file_name, '') AS source_file_name,
  user_agent_required,
  config,
  capabilities,
  requires_translation,
  created_at,
  updated_at
FROM data_sources
WHERE name = $1;

-- name: ListSources :many
SELECT
  id,
  name,
  country,
  source,
  registry_key,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  auth_required,
  storage_kind,
  clickhouse_database,
  clickhouse_table_prefix,
  COALESCE(source_url, '') AS source_url,
  COALESCE(docs_url, '') AS docs_url,
  COALESCE(raw_source_retention, '') AS raw_source_retention,
  COALESCE(source_file_name, '') AS source_file_name,
  user_agent_required,
  config,
  capabilities,
  requires_translation,
  created_at,
  updated_at
FROM data_sources
ORDER BY name;

-- name: GetSourcesWithCapabilities :many
SELECT
  id,
  name,
  country,
  source,
  registry_key,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  auth_required,
  storage_kind,
  clickhouse_database,
  clickhouse_table_prefix,
  COALESCE(source_url, '') AS source_url,
  COALESCE(docs_url, '') AS docs_url,
  COALESCE(raw_source_retention, '') AS raw_source_retention,
  COALESCE(source_file_name, '') AS source_file_name,
  user_agent_required,
  config,
  capabilities,
  requires_translation,
  created_at,
  updated_at
FROM data_sources
WHERE array_length(capabilities, 1) > 0
ORDER BY name;

-- name: ListSourceActions :many
SELECT
  a.id,
  a.source_id,
  s.name AS source_name,
  a.action,
  a.display_name,
  a.temporal_workflow_type,
  a.temporal_task_queue,
  a.enabled,
  a.config,
  a.created_at,
  a.updated_at
FROM data_source_actions a
JOIN data_sources s ON s.id = a.source_id
WHERE s.name = $1
ORDER BY a.action;

-- name: ListSourceActionRuns :many
SELECT
  r.id,
  r.source_id,
  s.name AS source_name,
  r.action_id,
  r.action,
  r.status,
  r.temporal_workflow_id,
  r.temporal_run_id,
  r.started_at,
  r.finished_at,
  r.input,
  r.result,
  r.error_message,
  r.created_at
FROM data_source_action_runs r
JOIN data_sources s ON s.id = r.source_id
WHERE s.name = $1
ORDER BY r.started_at DESC
LIMIT $2;

-- name: GetSourceActionByName :one
SELECT
  a.id,
  a.source_id,
  s.name AS source_name,
  s.country,
  s.source,
  s.registry_key,
  COALESCE(s.source_url, '') AS source_url,
  COALESCE(s.source_file_name, '') AS source_file_name,
  s.user_agent_required,
  a.action,
  a.display_name,
  a.temporal_workflow_type,
  a.temporal_task_queue,
  a.enabled,
  a.config
FROM data_source_actions a
JOIN data_sources s ON s.id = a.source_id
WHERE s.name = $1
  AND a.action = $2;

-- name: CreateSourceActionRun :one
INSERT INTO data_source_action_runs (
  id,
  source_id,
  action_id,
  action,
  status,
  temporal_workflow_id,
  temporal_run_id,
  input,
  result
)
SELECT
  sqlc.arg(id),
  a.source_id,
  a.id,
  a.action,
  'running',
  sqlc.arg(temporal_workflow_id),
  sqlc.arg(temporal_run_id),
  sqlc.arg(input),
  '{}'::jsonb
FROM data_source_actions a
WHERE a.id = sqlc.arg(action_id)
RETURNING *;

-- name: GetSourceActionRun :one
SELECT * FROM data_source_action_runs WHERE id = $1;

-- name: UpdateSourceActionRunTemporalRunID :exec
UPDATE data_source_action_runs
SET temporal_run_id = sqlc.arg(temporal_run_id)
WHERE id = sqlc.arg(id);

-- name: GetLatestSuccessfulSourceActionRun :one
SELECT r.*
FROM data_source_action_runs r
JOIN data_source_actions a ON a.id = r.action_id
JOIN data_sources s ON s.id = r.source_id
WHERE s.name = $1
  AND r.action = $2
  AND r.status = 'succeeded'
ORDER BY r.finished_at DESC
LIMIT 1;

-- name: FinishSourceActionRun :one
UPDATE data_source_action_runs
SET
  status = sqlc.arg(status),
  finished_at = now(),
  result = sqlc.arg(result),
  error_message = NULLIF(sqlc.arg(error_message)::text, '')
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: UpsertDataSourceFromCatalog :exec
INSERT INTO data_sources (
  name,
  country,
  source,
  registry_key,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  auth_required,
  storage_kind,
  clickhouse_database,
  clickhouse_table_prefix,
  source_url,
  docs_url,
  raw_source_retention,
  source_file_name,
  user_agent_required,
  capabilities,
  requires_translation,
  config
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
  $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
  '{}'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
  country = EXCLUDED.country,
  source = EXCLUDED.source,
  registry_key = EXCLUDED.registry_key,
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  source_group = EXCLUDED.source_group,
  input_table_name = EXCLUDED.input_table_name,
  enabled = EXCLUDED.enabled,
  auth_required = EXCLUDED.auth_required,
  storage_kind = EXCLUDED.storage_kind,
  clickhouse_database = EXCLUDED.clickhouse_database,
  clickhouse_table_prefix = EXCLUDED.clickhouse_table_prefix,
  source_url = EXCLUDED.source_url,
  docs_url = EXCLUDED.docs_url,
  raw_source_retention = EXCLUDED.raw_source_retention,
  source_file_name = EXCLUDED.source_file_name,
  user_agent_required = EXCLUDED.user_agent_required,
  capabilities = EXCLUDED.capabilities,
  requires_translation = EXCLUDED.requires_translation,
  config = '{}'::jsonb,
  updated_at = now();

-- name: UpsertDataSourceFileFromCatalog :exec
INSERT INTO data_source_files (
  source_id,
  file_key,
  display_name,
  description,
  kind,
  required,
  relative_path,
  enabled,
  sort_order,
  config
)
SELECT
  s.id,
  sqlc.arg(file_key),
  sqlc.arg(display_name),
  NULLIF(sqlc.arg(description)::text, ''),
  sqlc.arg(kind),
  sqlc.arg(required),
  sqlc.arg(relative_path),
  sqlc.arg(enabled),
  sqlc.arg(sort_order),
  sqlc.arg(config)
FROM data_sources s
WHERE s.registry_key = sqlc.arg(registry_key)
ON CONFLICT (source_id, file_key) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  kind = EXCLUDED.kind,
  required = EXCLUDED.required,
  relative_path = EXCLUDED.relative_path,
  enabled = EXCLUDED.enabled,
  sort_order = EXCLUDED.sort_order,
  config = EXCLUDED.config,
  updated_at = now();

-- name: DisableDataSourceFilesNotInCatalog :exec
UPDATE data_source_files f
SET enabled = false, updated_at = now()
FROM data_sources s
WHERE s.id = f.source_id
  AND s.registry_key = sqlc.arg(registry_key)
  AND NOT (f.file_key = ANY(sqlc.arg(file_keys)::text[]));

-- name: PruneDataSourcesNotInCatalog :exec
DELETE FROM data_sources
WHERE NOT (registry_key = ANY($1::text[]));

-- name: ListSourceFilesWithLatestRun :many
WITH latest AS (
  SELECT DISTINCT ON (r.source_file_id)
    r.*
  FROM data_source_file_runs r
  ORDER BY r.source_file_id, r.started_at DESC
),
latest_success AS (
  SELECT DISTINCT ON (r.source_file_id)
    r.*
  FROM data_source_file_runs r
  WHERE r.status = 'succeeded'
  ORDER BY r.source_file_id, r.finished_at DESC NULLS LAST, r.started_at DESC
)
SELECT
  f.id,
  f.source_id,
  s.name AS source_name,
  f.file_key,
  f.display_name,
  f.description,
  f.kind,
  f.required,
  f.relative_path,
  f.enabled,
  f.sort_order,
  f.config,
  f.created_at,
  f.updated_at,
  latest.id AS latest_run_id,
  latest.status AS latest_status,
  latest.started_at AS latest_started_at,
  latest.finished_at AS latest_finished_at,
  latest.path AS latest_path,
  latest.content_sha256 AS latest_content_sha256,
  latest.content_length_bytes AS latest_content_length_bytes,
  latest.records_written AS latest_records_written,
  latest.error_message AS latest_error_message,
  latest_success.id AS latest_successful_run_id,
  latest_success.path AS latest_successful_path
FROM data_source_files f
JOIN data_sources s ON s.id = f.source_id
LEFT JOIN latest ON latest.source_file_id = f.id
LEFT JOIN latest_success ON latest_success.source_file_id = f.id
WHERE s.name = $1
ORDER BY f.sort_order, f.file_key;

-- name: GetSourceFileBySourceNameAndKey :one
SELECT
  f.*,
  s.name AS source_name,
  s.country,
  s.source,
  s.registry_key,
  COALESCE(s.source_url, '') AS source_url,
  s.user_agent_required
FROM data_source_files f
JOIN data_sources s ON s.id = f.source_id
WHERE s.name = $1
  AND f.file_key = $2
  AND f.enabled = true;

-- name: CreateSourceFileRun :one
INSERT INTO data_source_file_runs (
  id,
  source_id,
  source_file_id,
  parent_action_run_id,
  status,
  temporal_workflow_id,
  temporal_run_id,
  log
)
SELECT
  sqlc.arg(id),
  f.source_id,
  f.id,
  sqlc.narg(parent_action_run_id),
  'running',
  sqlc.arg(temporal_workflow_id),
  sqlc.narg(temporal_run_id),
  '[]'::jsonb
FROM data_source_files f
LEFT JOIN data_source_action_runs parent
  ON parent.id = sqlc.narg(parent_action_run_id)::uuid
 AND parent.source_id = f.source_id
WHERE f.id = sqlc.arg(source_file_id)
  AND (
    sqlc.narg(parent_action_run_id)::uuid IS NULL
    OR parent.id IS NOT NULL
  )
RETURNING *;

-- name: UpdateSourceFileRunTemporalRunID :exec
UPDATE data_source_file_runs
SET temporal_run_id = sqlc.arg(temporal_run_id)
WHERE id = sqlc.arg(id);

-- name: FinishSourceFileRun :one
UPDATE data_source_file_runs
SET
  status = sqlc.arg(status),
  finished_at = now(),
  path = NULLIF(sqlc.arg(path)::text, ''),
  content_sha256 = NULLIF(sqlc.arg(content_sha256)::text, ''),
  content_length_bytes = sqlc.narg(content_length_bytes),
  records_written = sqlc.narg(records_written),
  error_message = NULLIF(sqlc.arg(error_message)::text, ''),
  log = sqlc.arg(log)
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: ListSourceFileRuns :many
SELECT
  r.*,
  f.file_key,
  f.display_name,
  s.name AS source_name
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
JOIN data_sources s ON s.id = r.source_id
WHERE s.name = sqlc.arg(source_name)
  AND f.file_key = sqlc.arg(file_key)
ORDER BY r.started_at DESC
LIMIT sqlc.arg(row_limit);

-- name: GetLatestSuccessfulSourceFileRun :one
SELECT
  r.*,
  f.file_key,
  f.kind,
  f.relative_path,
  f.required,
  f.config,
  s.name AS source_name,
  s.country,
  s.source,
  COALESCE(s.source_url, '') AS source_url,
  s.user_agent_required
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
JOIN data_sources s ON s.id = r.source_id
WHERE s.name = sqlc.arg(source_name)
  AND f.file_key = sqlc.arg(file_key)
  AND r.status = 'succeeded'
  AND r.path IS NOT NULL
ORDER BY r.finished_at DESC NULLS LAST, r.started_at DESC
LIMIT 1;

-- name: ListSuccessfulSourceFileRunsForAction :many
SELECT
  r.*,
  f.file_key,
  f.kind,
  f.relative_path,
  f.required,
  f.config
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
WHERE r.parent_action_run_id = $1
  AND r.status = 'succeeded'
ORDER BY f.sort_order, f.file_key;

-- name: ListLatestSuccessfulRequiredSourceFileRuns :many
WITH latest AS (
  SELECT DISTINCT ON (f.id)
    r.*,
    f.file_key,
    f.kind,
    f.relative_path,
    f.required,
    f.config
  FROM data_source_files f
  JOIN data_sources s ON s.id = f.source_id
  LEFT JOIN data_source_file_runs r
    ON r.source_file_id = f.id
   AND r.status = 'succeeded'
  WHERE s.name = $1
    AND f.enabled = true
    AND f.required = true
  ORDER BY f.id, r.finished_at DESC NULLS LAST, r.started_at DESC
)
SELECT *
FROM latest
ORDER BY file_key;

-- name: GetSourceFileRunWithDefinition :one
SELECT
  r.*,
  f.file_key,
  f.kind,
  f.relative_path,
  f.required,
  f.config,
  s.name AS source_name,
  s.country,
  s.source,
  COALESCE(s.source_url, '') AS source_url,
  s.user_agent_required
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
JOIN data_sources s ON s.id = r.source_id
WHERE r.id = $1;
