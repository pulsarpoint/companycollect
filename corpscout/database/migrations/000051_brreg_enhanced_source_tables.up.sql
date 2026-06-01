CREATE TABLE brreg_enhanced_raw_inputs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,
  organization_number TEXT NOT NULL,
  payload_hash TEXT NOT NULL,

  enhancement_version TEXT NOT NULL DEFAULT 'brreg.enhanced.v1',
  attempt INTEGER NOT NULL DEFAULT 1,
  orchestrator_run_id TEXT,
  asset_key TEXT,

  status TEXT NOT NULL DEFAULT 'queued',
  section_statuses JSONB NOT NULL DEFAULT '{}'::jsonb,
  enhanced_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  started_at TIMESTAMPTZ,
  enhanced_at TIMESTAMPTZ,
  superseded_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_enhanced_status CHECK (
    status IN ('queued', 'running', 'succeeded', 'partial', 'failed', 'cancelled', 'superseded')
  ),
  CONSTRAINT chk_brreg_enhanced_attempt CHECK (attempt > 0),
  CONSTRAINT chk_brreg_enhanced_section_statuses_object CHECK (jsonb_typeof(section_statuses) = 'object'),
  CONSTRAINT chk_brreg_enhanced_payload_object CHECK (jsonb_typeof(enhanced_payload) = 'object'),
  CONSTRAINT chk_brreg_enhanced_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_input_id, payload_hash, enhancement_version, attempt)
);

CREATE INDEX idx_brreg_enhanced_raw_input
  ON brreg_enhanced_raw_inputs(raw_input_id, created_at DESC);

CREATE INDEX idx_brreg_enhanced_org_status
  ON brreg_enhanced_raw_inputs(organization_number, status, created_at DESC);

CREATE INDEX idx_brreg_enhanced_status_created
  ON brreg_enhanced_raw_inputs(status, created_at DESC);

CREATE INDEX idx_brreg_enhanced_orchestrator_run
  ON brreg_enhanced_raw_inputs(orchestrator_run_id)
  WHERE orchestrator_run_id IS NOT NULL;

CREATE INDEX idx_brreg_enhanced_latest_usable
  ON brreg_enhanced_raw_inputs(organization_number, enhancement_version, enhanced_at DESC)
  WHERE status IN ('succeeded', 'partial');

CREATE TABLE brreg_source_companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  enhanced_raw_input_id UUID NOT NULL REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE CASCADE,
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,

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

CREATE INDEX idx_brreg_source_companies_raw_input
  ON brreg_source_companies(raw_input_id);

CREATE INDEX idx_brreg_source_companies_enhanced
  ON brreg_source_companies(enhanced_raw_input_id);

CREATE INDEX idx_brreg_source_companies_name
  ON brreg_source_companies(name);

CREATE INDEX idx_brreg_source_companies_status_created
  ON brreg_source_companies(row_status, created_at DESC);

CREATE TABLE brreg_source_addresses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_raw_input_id UUID NOT NULL REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE CASCADE,
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,

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

CREATE INDEX idx_brreg_source_addresses_company
  ON brreg_source_addresses(source_company_id);

CREATE INDEX idx_brreg_source_addresses_postal_code
  ON brreg_source_addresses(postal_code)
  WHERE postal_code IS NOT NULL;

CREATE INDEX idx_brreg_source_addresses_municipality
  ON brreg_source_addresses(municipality_number)
  WHERE municipality_number IS NOT NULL;

CREATE TABLE brreg_source_industries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_raw_input_id UUID NOT NULL REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE CASCADE,
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,

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

CREATE INDEX idx_brreg_source_industries_company
  ON brreg_source_industries(source_company_id);

CREATE INDEX idx_brreg_source_industries_code
  ON brreg_source_industries(classification_type, code);

CREATE TABLE brreg_source_capital (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_raw_input_id UUID NOT NULL REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE CASCADE,
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,

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

CREATE INDEX idx_brreg_source_capital_company
  ON brreg_source_capital(source_company_id);

CREATE INDEX idx_brreg_source_capital_currency
  ON brreg_source_capital(original_currency)
  WHERE original_currency IS NOT NULL;

CREATE TABLE brreg_source_domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_raw_input_id UUID REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE SET NULL,
  raw_input_id UUID REFERENCES brreg_company_raw_inputs(id) ON DELETE SET NULL,

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

CREATE INDEX idx_brreg_source_domains_company_status
  ON brreg_source_domains(source_company_id, status);

CREATE INDEX idx_brreg_source_domains_domain
  ON brreg_source_domains(normalized_domain);

CREATE INDEX idx_brreg_source_domains_confidence
  ON brreg_source_domains(confidence DESC)
  WHERE status = 'active';

CREATE TABLE brreg_source_financials (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source_company_id UUID NOT NULL REFERENCES brreg_source_companies(id) ON DELETE CASCADE,
  enhanced_raw_input_id UUID NOT NULL REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE CASCADE,
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,

  fiscal_year INTEGER NOT NULL,
  period_start DATE,
  period_end DATE,
  statement_type TEXT NOT NULL DEFAULT 'annual_accounts',
  currency TEXT,
  is_consolidated BOOLEAN NOT NULL DEFAULT false,

  revenue_amount NUMERIC(20, 2),
  operating_profit_amount NUMERIC(20, 2),
  profit_before_tax_amount NUMERIC(20, 2),
  net_income_amount NUMERIC(20, 2),
  total_assets_amount NUMERIC(20, 2),
  total_equity_amount NUMERIC(20, 2),
  total_liabilities_amount NUMERIC(20, 2),

  facts JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_section JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_financials_year CHECK (fiscal_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_brreg_source_financials_facts_object CHECK (jsonb_typeof(facts) = 'object'),
  CONSTRAINT chk_brreg_source_financials_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_financials_raw_section_object CHECK (jsonb_typeof(raw_section) = 'object'),
  CONSTRAINT chk_brreg_source_financials_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (source_company_id, fiscal_year, statement_type, is_consolidated)
);

CREATE INDEX idx_brreg_source_financials_company_year
  ON brreg_source_financials(source_company_id, fiscal_year DESC);

CREATE INDEX idx_brreg_source_financials_year
  ON brreg_source_financials(fiscal_year DESC);

CREATE INDEX idx_brreg_source_financials_revenue
  ON brreg_source_financials(revenue_amount DESC)
  WHERE revenue_amount IS NOT NULL;

GRANT SELECT ON brreg_enhanced_raw_inputs TO corpscout_anon;
GRANT SELECT ON brreg_source_companies TO corpscout_anon;
GRANT SELECT ON brreg_source_addresses TO corpscout_anon;
GRANT SELECT ON brreg_source_industries TO corpscout_anon;
GRANT SELECT ON brreg_source_capital TO corpscout_anon;
GRANT SELECT ON brreg_source_domains TO corpscout_anon;
GRANT SELECT ON brreg_source_financials TO corpscout_anon;
