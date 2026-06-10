-- name: UpsertNACEClassification :one
INSERT INTO nace_classifications (
  code_system,
  revision,
  name,
  valid_from,
  valid_to,
  source_url,
  source_metadata
) VALUES ('NACE', $1, $2, $3, $4, $5, $6)
ON CONFLICT (code_system, revision)
DO UPDATE SET
  name = EXCLUDED.name,
  valid_from = EXCLUDED.valid_from,
  valid_to = EXCLUDED.valid_to,
  source_url = EXCLUDED.source_url,
  source_metadata = EXCLUDED.source_metadata,
  updated_at = now()
RETURNING *;

-- name: UpsertNACECode :one
INSERT INTO nace_codes (
  classification_id,
  code,
  normalized_code,
  level,
  level_name,
  parent_code,
  title,
  description,
  includes,
  excludes,
  notes,
  source_payload,
  source_hash,
  active
) VALUES (
  $1, $2, $3, $4, $5, $6, $7,
  $8, $9, $10, $11, $12, $13, true
)
ON CONFLICT (classification_id, code)
DO UPDATE SET
  normalized_code = EXCLUDED.normalized_code,
  level = EXCLUDED.level,
  level_name = EXCLUDED.level_name,
  parent_code = EXCLUDED.parent_code,
  title = EXCLUDED.title,
  description = EXCLUDED.description,
  includes = EXCLUDED.includes,
  excludes = EXCLUDED.excludes,
  notes = EXCLUDED.notes,
  source_payload = EXCLUDED.source_payload,
  source_hash = EXCLUDED.source_hash,
  active = true,
  updated_at = CASE
    WHEN nace_codes.source_hash IS DISTINCT FROM EXCLUDED.source_hash THEN now()
    ELSE nace_codes.updated_at
  END
RETURNING *;

-- name: LinkNACECodeParents :exec
UPDATE nace_codes child
SET parent_id = parent.id,
    updated_at = CASE
      WHEN child.parent_id IS DISTINCT FROM parent.id THEN now()
      ELSE child.updated_at
    END
FROM nace_codes parent
WHERE child.classification_id = $1
  AND parent.classification_id = child.classification_id
  AND child.parent_code IS NOT NULL
  AND (
    child.parent_code = parent.code
    OR regexp_replace(upper(child.parent_code), '[^0-9A-Z]', '', 'g') = parent.normalized_code
  );

-- name: ClearRootNACECodeParents :exec
UPDATE nace_codes
SET parent_id = NULL,
    updated_at = now()
WHERE classification_id = $1
  AND parent_code IS NULL
  AND parent_id IS NOT NULL;

-- name: DeactivateMissingNACECodes :one
WITH active_input_codes AS (
  SELECT unnest(sqlc.arg('active_codes')::text[]) AS code
),
updated AS (
  UPDATE nace_codes nc
  SET active = false,
      updated_at = now()
  WHERE nc.classification_id = sqlc.arg('classification_id')::uuid
    AND nc.active
    AND NOT EXISTS (
      SELECT 1 FROM active_input_codes input_codes WHERE input_codes.code = nc.code
    )
  RETURNING 1
)
SELECT count(*)::integer AS deactivated_count FROM updated;

-- name: UpsertNACECodeAlias :exec
INSERT INTO nace_code_aliases (
  nace_code_id,
  alias_type,
  alias_code,
  normalized_alias_code,
  source,
  metadata
) VALUES ($1, $2, $3, $4, 'nace', $5)
ON CONFLICT (nace_code_id, alias_type, alias_code)
DO UPDATE SET
  normalized_alias_code = EXCLUDED.normalized_alias_code,
  metadata = EXCLUDED.metadata;

-- name: GetNACEClassificationByRevision :one
SELECT * FROM nace_classifications
WHERE code_system = 'NACE'
  AND revision = $1;

-- name: GetNACECodeByRevisionAndCode :one
SELECT ncodes.*
FROM nace_codes ncodes
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
  AND nclass.revision = $1
  AND ncodes.code = $2
  AND ncodes.active;

-- name: ResolveNACECodeAlias :one
SELECT ncodes.*
FROM nace_code_aliases aliases
JOIN nace_codes ncodes ON ncodes.id = aliases.nace_code_id
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
  AND nclass.revision = $1
  AND aliases.source = 'nace'
  AND aliases.normalized_alias_code = $2
  AND ncodes.active
ORDER BY ncodes.level DESC
LIMIT 1;

-- name: ListNACECodeTree :many
SELECT * FROM v_nace_code_tree
WHERE code_system = 'NACE'
  AND revision = $1
  AND active = true
ORDER BY level, code;

-- name: ListNACECodeChildren :many
SELECT
  ncodes.id,
  ncodes.classification_id,
  ncodes.code,
  ncodes.normalized_code,
  ncodes.level,
  ncodes.level_name,
  ncodes.parent_code,
  ncodes.parent_id,
  ncodes.title,
  ncodes.description,
  ncodes.includes,
  ncodes.excludes,
  ncodes.active,
  ncodes.created_at,
  ncodes.updated_at,
  EXISTS (
    SELECT 1
    FROM nace_codes child
    WHERE child.parent_id = ncodes.id
      AND child.active
  ) AS has_children
FROM nace_codes ncodes
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
  AND nclass.revision = sqlc.arg('revision')::text
  AND ncodes.active
  AND (
    (sqlc.narg('parent_id')::uuid IS NULL AND ncodes.parent_id IS NULL)
    OR ncodes.parent_id = sqlc.narg('parent_id')::uuid
  )
ORDER BY ncodes.code;

-- name: ListNACETaxonomyState :many
SELECT * FROM v_nace_taxonomy_state
ORDER BY revision;

-- name: BeginNACEImportRun :one
INSERT INTO nace_import_runs (
  temporal_workflow_id,
  revision,
  source_url,
  metadata
) VALUES (
  sqlc.arg('temporal_workflow_id'),
  sqlc.arg('revision'),
  sqlc.arg('source_url'),
  COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (temporal_workflow_id)
DO UPDATE SET
  revision = EXCLUDED.revision,
  source_url = EXCLUDED.source_url,
  status = 'running',
  source_file_id = NULL,
  content_sha256 = NULL,
  records_seen = 0,
  records_imported = 0,
  records_deactivated = 0,
  started_at = now(),
  finished_at = NULL,
  error = NULL,
  metadata = EXCLUDED.metadata
RETURNING *;

-- name: GetNACEImportRunByWorkflowID :one
SELECT *
FROM nace_import_runs
WHERE temporal_workflow_id = $1;

-- name: GetProcessedNACESourceFileByHash :one
SELECT *
FROM nace_source_files
WHERE revision = sqlc.arg('revision')
  AND source_url = sqlc.arg('source_url')
  AND content_sha256 = sqlc.arg('content_sha256')
  AND status = 'processed';

-- name: UpsertDownloadedNACESourceFile :one
INSERT INTO nace_source_files (
  revision,
  source_url,
  content_sha256,
  content_length_bytes,
  content_type,
  etag,
  last_modified,
  status,
  metadata
) VALUES (
  sqlc.arg('revision'),
  sqlc.arg('source_url'),
  sqlc.arg('content_sha256'),
  sqlc.arg('content_length_bytes'),
  sqlc.narg('content_type'),
  sqlc.narg('etag'),
  sqlc.narg('last_modified'),
  'downloaded',
  COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (revision, source_url, content_sha256)
DO UPDATE SET
  content_length_bytes = EXCLUDED.content_length_bytes,
  content_type = EXCLUDED.content_type,
  etag = EXCLUDED.etag,
  last_modified = EXCLUDED.last_modified,
  status = CASE
    WHEN nace_source_files.status = 'processed' THEN nace_source_files.status
    ELSE 'downloaded'
  END,
  error = NULL,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING *;

-- name: MarkNACESourceFileProcessing :one
UPDATE nace_source_files
SET status = 'processing',
    error = NULL,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: MarkNACESourceFileProcessed :one
UPDATE nace_source_files
SET status = 'processed',
    processed_at = now(),
    error = NULL,
    metadata = metadata || COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb),
    updated_at = now()
WHERE id = sqlc.arg('id')
RETURNING *;

-- name: MarkNACESourceFileFailed :one
UPDATE nace_source_files
SET status = 'failed',
    error = sqlc.arg('error'),
    metadata = metadata || COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb),
    updated_at = now()
WHERE id = sqlc.arg('id')
RETURNING *;

-- name: FinishNACEImportRun :one
UPDATE nace_import_runs
SET
  source_file_id = sqlc.narg('source_file_id')::uuid,
  status = sqlc.arg('status'),
  content_sha256 = sqlc.narg('content_sha256'),
  records_seen = sqlc.arg('records_seen'),
  records_imported = sqlc.arg('records_imported'),
  records_deactivated = sqlc.arg('records_deactivated'),
  finished_at = now(),
  error = NULLIF(sqlc.arg('error')::text, ''),
  metadata = metadata || COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
WHERE id = sqlc.arg('id')
RETURNING *;

-- name: ListNACESourceFileImports :many
SELECT *
FROM v_nace_source_file_imports
WHERE revision = COALESCE(sqlc.narg('revision')::text, revision)
ORDER BY started_at DESC
LIMIT sqlc.arg('limit')::integer;

-- name: ListNACEClassificationsForClickHouse :many
SELECT
  nclass.revision,
  nclass.name,
  nclass.valid_from,
  nclass.valid_to,
  nclass.source_url,
  count(ncodes.id) FILTER (WHERE ncodes.active) AS active_codes,
  count(ncodes.id) FILTER (WHERE NOT ncodes.active) AS inactive_codes
FROM nace_classifications nclass
LEFT JOIN nace_codes ncodes ON ncodes.classification_id = nclass.id
WHERE nclass.code_system = 'NACE'
GROUP BY nclass.id
ORDER BY nclass.revision;

-- name: ListNACECodesForClickHouse :many
SELECT
  nclass.revision,
  ncodes.id,
  ncodes.code,
  ncodes.normalized_code,
  ncodes.level,
  ncodes.level_name,
  ncodes.parent_code,
  parent.normalized_code AS parent_normalized_code,
  ncodes.parent_id,
  ncodes.title,
  ncodes.description,
  ncodes.active
FROM nace_codes ncodes
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
LEFT JOIN nace_codes parent ON parent.id = ncodes.parent_id
WHERE nclass.code_system = 'NACE'
ORDER BY nclass.revision, ncodes.level, ncodes.code;

-- name: ListNACECodeAliasesForClickHouse :many
SELECT
  nclass.revision,
  ncodes.code,
  aliases.alias_type,
  aliases.alias_code,
  aliases.normalized_alias_code
FROM nace_code_aliases aliases
JOIN nace_codes ncodes ON ncodes.id = aliases.nace_code_id
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
ORDER BY nclass.revision, aliases.alias_type, aliases.normalized_alias_code, ncodes.code;
