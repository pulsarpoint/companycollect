CREATE TABLE llm_providers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug TEXT NOT NULL,
  display_name TEXT NOT NULL,
  provider_type TEXT NOT NULL DEFAULT 'openai_compatible',
  base_url TEXT NOT NULL,
  model TEXT NOT NULL,
  api_key_ciphertext TEXT,
  api_key_nonce TEXT,
  api_key_key_version TEXT NOT NULL DEFAULT 'v1',
  api_key_algorithm TEXT NOT NULL DEFAULT 'aes-256-gcm',
  is_default BOOLEAN NOT NULL DEFAULT false,
  enabled BOOLEAN NOT NULL DEFAULT false,
  capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_llm_providers_slug CHECK (slug ~ '^[a-z0-9][a-z0-9_-]{1,62}[a-z0-9]$'),
  CONSTRAINT chk_llm_providers_provider_type CHECK (provider_type IN ('openai_compatible')),
  CONSTRAINT chk_llm_providers_base_url CHECK (base_url ~ '^https?://'),
  CONSTRAINT chk_llm_providers_key_pair CHECK (
    (api_key_ciphertext IS NULL AND api_key_nonce IS NULL)
    OR
    (api_key_ciphertext IS NOT NULL AND api_key_nonce IS NOT NULL)
  ),
  CONSTRAINT chk_llm_providers_key_algorithm CHECK (api_key_algorithm IN ('aes-256-gcm')),
  CONSTRAINT chk_llm_providers_capabilities_object CHECK (jsonb_typeof(capabilities) = 'object'),
  CONSTRAINT chk_llm_providers_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX ux_llm_providers_slug ON llm_providers(slug);
CREATE UNIQUE INDEX ux_llm_providers_enabled_default ON llm_providers(is_default) WHERE is_default;
CREATE INDEX idx_llm_providers_enabled ON llm_providers(enabled, display_name);
