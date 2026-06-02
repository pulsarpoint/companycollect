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
  AND child.parent_code = parent.code;

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

-- name: ListNACETaxonomyState :many
SELECT * FROM v_nace_taxonomy_state
ORDER BY revision;
