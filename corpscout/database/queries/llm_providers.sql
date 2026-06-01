-- name: ListLLMProviders :many
SELECT
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  (api_key_ciphertext IS NOT NULL)::boolean AS has_api_key,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at
FROM llm_providers
ORDER BY is_default DESC, display_name ASC, slug ASC;

-- name: ListEnabledLLMProviders :many
SELECT
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  (api_key_ciphertext IS NOT NULL)::boolean AS has_api_key,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at
FROM llm_providers
WHERE enabled
ORDER BY is_default DESC, display_name ASC, slug ASC;

-- name: GetLLMProviderForUse :one
SELECT
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  api_key_ciphertext,
  api_key_nonce,
  api_key_key_version,
  api_key_algorithm,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at
FROM llm_providers
WHERE id = sqlc.arg('id')::uuid;

-- name: GetLLMProviderBySlugForUse :one
SELECT
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  api_key_ciphertext,
  api_key_nonce,
  api_key_key_version,
  api_key_algorithm,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at
FROM llm_providers
WHERE slug = sqlc.arg('slug')::text;

-- name: CreateLLMProvider :one
INSERT INTO llm_providers (
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  api_key_ciphertext,
  api_key_nonce,
  api_key_key_version,
  api_key_algorithm,
  is_default,
  enabled,
  capabilities,
  metadata
) VALUES (
  sqlc.arg('slug')::text,
  sqlc.arg('display_name')::text,
  sqlc.arg('provider_type')::text,
  sqlc.arg('base_url')::text,
  sqlc.arg('model')::text,
  sqlc.narg('api_key_ciphertext')::text,
  sqlc.narg('api_key_nonce')::text,
  sqlc.arg('api_key_key_version')::text,
  sqlc.arg('api_key_algorithm')::text,
  sqlc.arg('is_default')::boolean,
  sqlc.arg('enabled')::boolean,
  COALESCE(sqlc.narg('capabilities')::jsonb, '{}'::jsonb),
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
RETURNING
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  (api_key_ciphertext IS NOT NULL)::boolean AS has_api_key,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at;

-- name: UpdateLLMProvider :one
UPDATE llm_providers
SET
  display_name = sqlc.arg('display_name')::text,
  provider_type = sqlc.arg('provider_type')::text,
  base_url = sqlc.arg('base_url')::text,
  model = sqlc.arg('model')::text,
  api_key_ciphertext = sqlc.narg('api_key_ciphertext')::text,
  api_key_nonce = sqlc.narg('api_key_nonce')::text,
  api_key_key_version = sqlc.arg('api_key_key_version')::text,
  api_key_algorithm = sqlc.arg('api_key_algorithm')::text,
  enabled = sqlc.arg('enabled')::boolean,
  capabilities = COALESCE(sqlc.narg('capabilities')::jsonb, '{}'::jsonb),
  metadata = COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE id = sqlc.arg('id')::uuid
RETURNING
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  (api_key_ciphertext IS NOT NULL)::boolean AS has_api_key,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at;

-- name: ClearDefaultLLMProvider :exec
UPDATE llm_providers
SET is_default = false,
    updated_at = now()
WHERE is_default;

-- name: SetDefaultLLMProvider :one
WITH cleared AS (
  UPDATE llm_providers
  SET is_default = false,
      updated_at = now()
  WHERE is_default
  RETURNING id
)
UPDATE llm_providers
SET is_default = true,
    enabled = true,
    updated_at = now()
WHERE id = sqlc.arg('id')::uuid
RETURNING
  id,
  slug,
  display_name,
  provider_type,
  base_url,
  model,
  (api_key_ciphertext IS NOT NULL)::boolean AS has_api_key,
  is_default,
  enabled,
  capabilities,
  metadata,
  created_at,
  updated_at;
