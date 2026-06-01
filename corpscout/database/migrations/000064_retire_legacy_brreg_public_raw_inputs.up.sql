DROP VIEW IF EXISTS v_source_raw_inputs;
DROP VIEW IF EXISTS v_brreg_raw_input_action_attributes;

ALTER TABLE brreg_workflow.enhanced_records
  DROP CONSTRAINT IF EXISTS enhanced_records_corpscout_enhanced_raw_input_id_fkey;
ALTER TABLE brreg_workflow.enhanced_records
  DROP COLUMN IF EXISTS corpscout_enhanced_raw_input_id;

UPDATE brreg_workflow.enhanced_records
SET corpscout_source_company_id = NULL
WHERE corpscout_source_company_id IS NOT NULL;

ALTER TABLE brreg_workflow.enhanced_records
  DROP CONSTRAINT IF EXISTS enhanced_records_corpscout_source_company_id_fkey;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM brreg_source_companies LIMIT 1)
     OR EXISTS (SELECT 1 FROM brreg_source_addresses LIMIT 1)
     OR EXISTS (SELECT 1 FROM brreg_source_industries LIMIT 1)
     OR EXISTS (SELECT 1 FROM brreg_source_capital LIMIT 1)
     OR EXISTS (SELECT 1 FROM brreg_source_domains LIMIT 1)
     OR EXISTS (SELECT 1 FROM brreg_source_financials LIMIT 1) THEN
    RAISE EXCEPTION 'Cannot rebuild BRREG source tables while normalized source tables contain data';
  END IF;
END $$;

DROP TABLE IF EXISTS brreg_source_financials;
DROP TABLE IF EXISTS brreg_source_domains;
DROP TABLE IF EXISTS brreg_source_capital;
DROP TABLE IF EXISTS brreg_source_industries;
DROP TABLE IF EXISTS brreg_source_addresses;
DROP TABLE IF EXISTS brreg_source_companies;

ALTER TABLE IF EXISTS brreg_enhanced_raw_inputs
  RENAME TO legacy_brreg_enhanced_raw_inputs;

ALTER TABLE IF EXISTS brreg_raw_input_domains
  RENAME TO legacy_brreg_raw_input_domains;

ALTER TABLE IF EXISTS brreg_raw_input_action_events
  RENAME TO legacy_brreg_raw_input_action_events;

ALTER TABLE IF EXISTS brreg_raw_input_actions
  RENAME TO legacy_brreg_raw_input_actions;

ALTER TABLE IF EXISTS brreg_company_raw_inputs
  RENAME TO legacy_brreg_company_raw_inputs;

CREATE TABLE brreg_source_companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  enhanced_record_id UUID NOT NULL REFERENCES brreg_workflow.enhanced_records(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,

  organization_number TEXT NOT NULL,
  name TEXT NOT NULL,
  registration_status TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'NO',
  website TEXT,
  phone TEXT,

  organization_form_code TEXT,
  organization_form_description TEXT,
  organization_form_description_en TEXT,
  language_code TEXT,
  response_class TEXT,

  founded_date DATE,
  unit_registry_registered_at DATE,
  enterprise_registry_registered_at DATE,
  vat_registry_registered_at DATE,
  vat_registry_unit_registered_at DATE,
  articles_date DATE,
  last_annual_report_year INTEGER,

  activity_description TEXT,
  activity_description_en TEXT,
  statutory_purpose TEXT,
  statutory_purpose_en TEXT,

  is_bankrupt BOOLEAN,
  is_in_group BOOLEAN,
  is_under_liquidation BOOLEAN,
  is_forced_dissolution BOOLEAN,
  has_registered_employees BOOLEAN,
  in_vat_register BOOLEAN,
  in_business_register BOOLEAN,
  in_voluntary_register BOOLEAN,
  in_foundation_register BOOLEAN,
  in_party_register BOOLEAN,

  source_updated_at TIMESTAMPTZ,
  payload_hash TEXT NOT NULL,
  enhancement_version TEXT NOT NULL,
  row_status TEXT NOT NULL DEFAULT 'active',
  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_section JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,

  CONSTRAINT chk_brreg_source_companies_status CHECK (row_status IN ('active', 'superseded')),
  CONSTRAINT chk_brreg_source_companies_normalized_payload_object CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_brreg_source_companies_raw_section_object CHECK (jsonb_typeof(raw_section) = 'object')
);

CREATE UNIQUE INDEX uq_brreg_source_companies_active_org
  ON brreg_source_companies(organization_number)
  WHERE row_status = 'active';
CREATE INDEX idx_brreg_source_companies_raw_record
  ON brreg_source_companies(raw_record_id);
CREATE INDEX idx_brreg_source_companies_enhanced_record
  ON brreg_source_companies(enhanced_record_id);
CREATE INDEX idx_brreg_source_companies_name
  ON brreg_source_companies(name);
CREATE INDEX idx_brreg_source_companies_status_created
  ON brreg_source_companies(row_status, created_at DESC);

CREATE TABLE brreg_source_addresses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_record_id UUID NOT NULL REFERENCES brreg_workflow.enhanced_records(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  address_type TEXT NOT NULL,
  street_lines TEXT[] NOT NULL DEFAULT '{}'::text[],
  postal_code TEXT,
  city TEXT,
  municipality TEXT,
  municipality_number TEXT,
  country TEXT,
  country_code TEXT,
  raw_section JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_addresses_type CHECK (address_type IN ('business', 'postal')),
  CONSTRAINT chk_brreg_source_addresses_raw_section_object CHECK (jsonb_typeof(raw_section) = 'object'),
  UNIQUE (source_company_id, address_type)
);

CREATE INDEX idx_brreg_source_addresses_company ON brreg_source_addresses(source_company_id);
CREATE INDEX idx_brreg_source_addresses_postal_code ON brreg_source_addresses(postal_code) WHERE postal_code IS NOT NULL;
CREATE INDEX idx_brreg_source_addresses_municipality ON brreg_source_addresses(municipality_number) WHERE municipality_number IS NOT NULL;

CREATE TABLE brreg_source_industries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_record_id UUID NOT NULL REFERENCES brreg_workflow.enhanced_records(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  classification_type TEXT NOT NULL,
  position SMALLINT NOT NULL DEFAULT 1,
  code TEXT NOT NULL,
  description TEXT,
  description_en TEXT,
  raw_section JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_industries_type CHECK (
    classification_type IN ('industry', 'helper_unit', 'institutional_sector')
  ),
  CONSTRAINT chk_brreg_source_industries_position CHECK (position BETWEEN 1 AND 10),
  CONSTRAINT chk_brreg_source_industries_raw_section_object CHECK (jsonb_typeof(raw_section) = 'object'),
  UNIQUE (source_company_id, classification_type, position)
);

CREATE INDEX idx_brreg_source_industries_company ON brreg_source_industries(source_company_id);
CREATE INDEX idx_brreg_source_industries_code ON brreg_source_industries(classification_type, code);

CREATE TABLE brreg_source_capital (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_record_id UUID NOT NULL REFERENCES brreg_workflow.enhanced_records(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  capital_type TEXT,
  capital_type_en TEXT,
  original_amount NUMERIC(20, 2),
  original_currency TEXT,
  introduced_at DATE,
  share_count INTEGER,
  amount_usd_cents BIGINT,
  fx_source TEXT,
  fx_rate_date DATE,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_section JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_capital_share_count CHECK (share_count IS NULL OR share_count >= 0),
  CONSTRAINT chk_brreg_source_capital_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_brreg_source_capital_raw_section_object CHECK (jsonb_typeof(raw_section) = 'object'),
  UNIQUE (source_company_id)
);

CREATE INDEX idx_brreg_source_capital_company ON brreg_source_capital(source_company_id);
CREATE INDEX idx_brreg_source_capital_currency ON brreg_source_capital(original_currency) WHERE original_currency IS NOT NULL;

CREATE TABLE brreg_source_domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_record_id UUID REFERENCES brreg_workflow.enhanced_records(id) ON DELETE SET NULL,
  raw_record_id UUID REFERENCES brreg_workflow.raw_records(id) ON DELETE SET NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  best_signal TEXT NOT NULL,
  confidence SMALLINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  source TEXT NOT NULL DEFAULT 'workflow',
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_at TIMESTAMPTZ,
  removed_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_domains_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_source_domains_status CHECK (status IN ('active', 'rejected', 'removed', 'superseded')),
  CONSTRAINT chk_brreg_source_domains_source CHECK (source IN ('workflow', 'manual', 'corpscout')),
  CONSTRAINT chk_brreg_source_domains_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_domains_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_brreg_source_domains_removed_fields CHECK (
    (status = 'removed' AND removed_at IS NOT NULL) OR (status <> 'removed' AND removed_at IS NULL)
  ),
  UNIQUE (source_company_id, normalized_domain)
);

CREATE INDEX idx_brreg_source_domains_company_status ON brreg_source_domains(source_company_id, status);
CREATE INDEX idx_brreg_source_domains_domain ON brreg_source_domains(normalized_domain);
CREATE INDEX idx_brreg_source_domains_confidence ON brreg_source_domains(confidence DESC) WHERE status = 'active';

CREATE TABLE brreg_source_financials (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_record_id UUID NOT NULL REFERENCES brreg_workflow.enhanced_records(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  fiscal_year INTEGER NOT NULL,
  period_start DATE,
  period_end DATE,
  statement_type TEXT NOT NULL DEFAULT 'annual_accounts',
  original_currency TEXT,
  is_consolidated BOOLEAN NOT NULL DEFAULT false,
  revenue_original_amount NUMERIC(20, 2),
  operating_profit_original_amount NUMERIC(20, 2),
  profit_before_tax_original_amount NUMERIC(20, 2),
  net_income_original_amount NUMERIC(20, 2),
  total_assets_original_amount NUMERIC(20, 2),
  total_equity_original_amount NUMERIC(20, 2),
  total_liabilities_original_amount NUMERIC(20, 2),
  fx_source TEXT,
  fx_rate_date DATE,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  revenue_usd_cents BIGINT,
  operating_profit_usd_cents BIGINT,
  profit_before_tax_usd_cents BIGINT,
  net_income_usd_cents BIGINT,
  total_assets_usd_cents BIGINT,
  total_equity_usd_cents BIGINT,
  total_liabilities_usd_cents BIGINT,
  facts JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_section JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_financials_year CHECK (fiscal_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_brreg_source_financials_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_brreg_source_financials_facts_object CHECK (jsonb_typeof(facts) = 'object'),
  CONSTRAINT chk_brreg_source_financials_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_financials_raw_section_object CHECK (jsonb_typeof(raw_section) = 'object'),
  CONSTRAINT chk_brreg_source_financials_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (source_company_id, fiscal_year, statement_type, is_consolidated)
);

CREATE INDEX idx_brreg_source_financials_company_year ON brreg_source_financials(source_company_id, fiscal_year DESC);
CREATE INDEX idx_brreg_source_financials_year ON brreg_source_financials(fiscal_year DESC);
CREATE INDEX idx_brreg_source_financials_revenue_original ON brreg_source_financials(revenue_original_amount DESC) WHERE revenue_original_amount IS NOT NULL;
CREATE INDEX idx_brreg_source_financials_revenue_usd ON brreg_source_financials(revenue_usd_cents DESC) WHERE revenue_usd_cents IS NOT NULL;

ALTER TABLE brreg_workflow.enhanced_records
  ADD CONSTRAINT enhanced_records_corpscout_source_company_id_fkey
  FOREIGN KEY (corpscout_source_company_id)
  REFERENCES brreg_source_companies(id) ON DELETE SET NULL;

GRANT SELECT ON brreg_source_companies TO corpscout_anon;
GRANT SELECT ON brreg_source_addresses TO corpscout_anon;
GRANT SELECT ON brreg_source_industries TO corpscout_anon;
GRANT SELECT ON brreg_source_capital TO corpscout_anon;
GRANT SELECT ON brreg_source_domains TO corpscout_anon;
GRANT SELECT ON brreg_source_financials TO corpscout_anon;

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
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
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
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM companies_house_company_raw_inputs chri
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
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
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
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
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
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
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
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM domain_discovery_raw_inputs ddri;

GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
