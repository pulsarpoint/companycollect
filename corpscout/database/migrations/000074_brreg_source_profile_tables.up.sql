CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS brreg_source;

CREATE TABLE brreg_source.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE RESTRICT,
  enhanced_record_id UUID REFERENCES brreg_workflow.enhanced_records(id) ON DELETE SET NULL,
  translation_result_id UUID REFERENCES brreg_workflow.translation_results(id) ON DELETE SET NULL,
  financial_result_id UUID REFERENCES brreg_workflow.financial_results(id) ON DELETE SET NULL,

  organization_number TEXT NOT NULL,
  source_native_id TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'NO',

  organization_name TEXT NOT NULL,
  organization_name_normalized TEXT NOT NULL,
  organization_name_en TEXT,
  short_description TEXT,
  short_description_en TEXT,
  description TEXT,
  description_en TEXT,

  registration_status TEXT,
  registration_status_label TEXT,
  registration_status_label_en TEXT,
  lifecycle_status TEXT NOT NULL DEFAULT 'active',

  organization_form_code TEXT,
  organization_form_label TEXT,
  organization_form_label_en TEXT,
  language_code TEXT,
  response_class TEXT,
  response_class_en TEXT,

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

  employee_count INTEGER,
  employee_count_source TEXT,
  employee_band TEXT,

  source_updated_at TIMESTAMPTZ,
  payload_hash TEXT NOT NULL,
  profile_version TEXT NOT NULL DEFAULT 'brreg.source_profile.v1',
  row_status TEXT NOT NULL DEFAULT 'active',

  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_company_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,

  CONSTRAINT chk_brreg_source_companies_status CHECK (row_status IN ('active', 'superseded')),
  CONSTRAINT chk_brreg_source_companies_lifecycle CHECK (
    lifecycle_status IN ('active', 'inactive', 'bankrupt', 'liquidating', 'forced_dissolution', 'unknown')
  ),
  CONSTRAINT chk_brreg_source_companies_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
  CONSTRAINT chk_brreg_source_companies_payload_object CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_brreg_source_companies_raw_object CHECK (jsonb_typeof(raw_company_payload) = 'object'),
  CONSTRAINT chk_brreg_source_companies_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_companies_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_brreg_source_companies_active_org
  ON brreg_source.companies(organization_number)
  WHERE row_status = 'active';

CREATE INDEX idx_brreg_source_companies_raw_record
  ON brreg_source.companies(raw_record_id);

CREATE INDEX idx_brreg_source_companies_name
  ON brreg_source.companies(organization_name_normalized);

CREATE INDEX idx_brreg_source_companies_status
  ON brreg_source.companies(row_status, lifecycle_status, updated_at DESC);

CREATE TABLE brreg_source.addresses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE RESTRICT,

  address_type TEXT NOT NULL,
  street_lines TEXT[] NOT NULL DEFAULT '{}'::text[],
  street_text TEXT,
  postal_code TEXT,
  city TEXT,
  municipality TEXT,
  municipality_number TEXT,
  county TEXT,
  county_number TEXT,
  country TEXT,
  country_en TEXT,
  country_code TEXT,
  formatted_address TEXT,

  latitude NUMERIC(10, 7),
  longitude NUMERIC(10, 7),
  coordinate_system TEXT,
  geocode_status TEXT NOT NULL DEFAULT 'not_attempted',
  geocode_provider TEXT,
  geocode_provider_place_id TEXT,
  geocode_confidence SMALLINT,
  geocode_precision TEXT,
  geocoded_at TIMESTAMPTZ,
  geocode_payload JSONB NOT NULL DEFAULT '{}'::jsonb,

  raw_address_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_addresses_type CHECK (address_type IN ('business', 'postal', 'other')),
  CONSTRAINT chk_brreg_source_addresses_geocode CHECK (
    geocode_status IN ('not_attempted', 'queued', 'running', 'succeeded', 'failed', 'not_found', 'ambiguous')
  ),
  CONSTRAINT chk_brreg_source_addresses_coordinates CHECK (
    (latitude IS NULL AND longitude IS NULL) OR
    (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
  ),
  CONSTRAINT chk_brreg_source_addresses_geocode_confidence CHECK (
    geocode_confidence IS NULL OR geocode_confidence BETWEEN 1 AND 100
  ),
  CONSTRAINT chk_brreg_source_addresses_geocode_payload_object CHECK (jsonb_typeof(geocode_payload) = 'object'),
  CONSTRAINT chk_brreg_source_addresses_raw_object CHECK (jsonb_typeof(raw_address_payload) = 'object'),
  CONSTRAINT chk_brreg_source_addresses_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_addresses_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (company_id, address_type)
);

CREATE INDEX idx_brreg_source_addresses_company
  ON brreg_source.addresses(company_id);

CREATE INDEX idx_brreg_source_addresses_postal_code
  ON brreg_source.addresses(postal_code)
  WHERE postal_code IS NOT NULL;

CREATE INDEX idx_brreg_source_addresses_municipality
  ON brreg_source.addresses(municipality_number)
  WHERE municipality_number IS NOT NULL;

CREATE INDEX idx_brreg_source_addresses_coordinates
  ON brreg_source.addresses(latitude, longitude)
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

CREATE INDEX idx_brreg_source_addresses_geocode_queue
  ON brreg_source.addresses(geocode_status, updated_at)
  WHERE geocode_status IN ('not_attempted', 'queued', 'failed', 'ambiguous');

CREATE TABLE brreg_source.industries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE RESTRICT,
  nace_code_id UUID REFERENCES nace_codes(id) ON DELETE RESTRICT,

  classification_type TEXT NOT NULL,
  source_field TEXT NOT NULL,
  position SMALLINT NOT NULL DEFAULT 1,
  source_code TEXT NOT NULL,
  source_label TEXT,
  source_label_en TEXT,

  mapped_nace_code TEXT,
  nace_revision TEXT,
  nace_title TEXT,
  nace_title_en TEXT,
  mapping_method TEXT,
  mapping_confidence REAL,

  is_primary BOOLEAN NOT NULL DEFAULT false,
  raw_industry_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_industries_type CHECK (
    classification_type IN ('industry', 'helper_unit', 'institutional_sector')
  ),
  CONSTRAINT chk_brreg_source_industries_position CHECK (position BETWEEN 1 AND 10),
  CONSTRAINT chk_brreg_source_industries_confidence CHECK (
    mapping_confidence IS NULL OR mapping_confidence BETWEEN 0 AND 1
  ),
  CONSTRAINT chk_brreg_source_industries_raw_object CHECK (jsonb_typeof(raw_industry_payload) = 'object'),
  CONSTRAINT chk_brreg_source_industries_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_industries_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (company_id, classification_type, position)
);

CREATE INDEX idx_brreg_source_industries_company
  ON brreg_source.industries(company_id);

CREATE INDEX idx_brreg_source_industries_source_code
  ON brreg_source.industries(classification_type, source_code);

CREATE INDEX idx_brreg_source_industries_nace
  ON brreg_source.industries(nace_code_id)
  WHERE nace_code_id IS NOT NULL;

CREATE TABLE brreg_source.websites (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES brreg_workflow.raw_records(id) ON DELETE SET NULL,
  domain_result_id UUID REFERENCES brreg_workflow.domain_results(id) ON DELETE SET NULL,

  url TEXT NOT NULL,
  normalized_url TEXT NOT NULL,
  host TEXT,
  path TEXT,
  website_type TEXT NOT NULL DEFAULT 'unknown',
  source TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT,
  is_primary BOOLEAN NOT NULL DEFAULT false,

  title TEXT,
  title_en TEXT,
  description TEXT,
  description_en TEXT,
  language_code TEXT,

  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_websites_type CHECK (
    website_type IN ('official_site', 'social_profile', 'marketplace', 'directory_profile', 'contact_page', 'other', 'unknown')
  ),
  CONSTRAINT chk_brreg_source_websites_source CHECK (
    source IN ('brreg', 'manual', 'domain_discovery', 'email', 'imported', 'other')
  ),
  CONSTRAINT chk_brreg_source_websites_status CHECK (
    status IN ('active', 'rejected', 'removed', 'superseded')
  ),
  CONSTRAINT chk_brreg_source_websites_confidence CHECK (confidence IS NULL OR confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_source_websites_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_websites_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_brreg_source_websites_active_url
  ON brreg_source.websites(company_id, normalized_url)
  WHERE status = 'active';

CREATE UNIQUE INDEX uq_brreg_source_websites_primary
  ON brreg_source.websites(company_id)
  WHERE status = 'active' AND is_primary;

CREATE INDEX idx_brreg_source_websites_host
  ON brreg_source.websites(host)
  WHERE host IS NOT NULL;

CREATE TABLE brreg_source.domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES brreg_workflow.raw_records(id) ON DELETE SET NULL,
  website_id UUID REFERENCES brreg_source.websites(id) ON DELETE SET NULL,
  domain_result_id UUID REFERENCES brreg_workflow.domain_results(id) ON DELETE SET NULL,

  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  registrable_domain TEXT NOT NULL,
  domain_type TEXT NOT NULL DEFAULT 'unknown',
  source TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT NOT NULL,
  is_primary BOOLEAN NOT NULL DEFAULT false,

  best_signal TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_domains_type CHECK (
    domain_type IN ('official', 'related', 'email_domain', 'infrastructure', 'unknown')
  ),
  CONSTRAINT chk_brreg_source_domains_source CHECK (
    source IN ('brreg_website', 'manual', 'domain_discovery', 'email', 'imported', 'other')
  ),
  CONSTRAINT chk_brreg_source_domains_status CHECK (
    status IN ('active', 'rejected', 'removed', 'superseded')
  ),
  CONSTRAINT chk_brreg_source_domains_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_source_domains_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_domains_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_brreg_source_domains_active
  ON brreg_source.domains(company_id, normalized_domain)
  WHERE status = 'active';

CREATE UNIQUE INDEX uq_brreg_source_domains_primary
  ON brreg_source.domains(company_id)
  WHERE status = 'active' AND is_primary;

CREATE INDEX idx_brreg_source_domains_domain
  ON brreg_source.domains(normalized_domain);

CREATE TABLE brreg_source.contacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES brreg_workflow.raw_records(id) ON DELETE SET NULL,
  website_id UUID REFERENCES brreg_source.websites(id) ON DELETE SET NULL,

  contact_type TEXT NOT NULL,
  value TEXT NOT NULL,
  normalized_value TEXT,
  label TEXT,
  label_en TEXT,
  source TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT,
  is_primary BOOLEAN NOT NULL DEFAULT false,

  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_contacts_type CHECK (
    contact_type IN ('phone', 'mobile', 'email', 'contact_form', 'fax', 'social_handle', 'other')
  ),
  CONSTRAINT chk_brreg_source_contacts_source CHECK (
    source IN ('brreg', 'manual', 'website', 'domain_discovery', 'imported', 'other')
  ),
  CONSTRAINT chk_brreg_source_contacts_status CHECK (
    status IN ('active', 'rejected', 'removed', 'superseded')
  ),
  CONSTRAINT chk_brreg_source_contacts_confidence CHECK (confidence IS NULL OR confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_source_contacts_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_contacts_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_brreg_source_contacts_active
  ON brreg_source.contacts(company_id, contact_type, normalized_value)
  WHERE status = 'active' AND normalized_value IS NOT NULL;

CREATE INDEX idx_brreg_source_contacts_company
  ON brreg_source.contacts(company_id, contact_type, status);

CREATE TABLE brreg_source.capital (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE RESTRICT,
  financial_result_id UUID REFERENCES brreg_workflow.financial_results(id) ON DELETE SET NULL,

  capital_type TEXT,
  capital_type_en TEXT,
  original_amount NUMERIC(20, 2),
  original_currency TEXT,
  amount_usd_cents BIGINT,
  fx_source TEXT,
  fx_rate_date DATE,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  introduced_at DATE,
  share_count INTEGER,

  raw_capital_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_capital_share_count CHECK (share_count IS NULL OR share_count >= 0),
  CONSTRAINT chk_brreg_source_capital_fx_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_brreg_source_capital_raw_object CHECK (jsonb_typeof(raw_capital_payload) = 'object'),
  CONSTRAINT chk_brreg_source_capital_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_capital_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (company_id)
);

CREATE TABLE brreg_source.financial_statements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE RESTRICT,
  financial_result_id UUID REFERENCES brreg_workflow.financial_results(id) ON DELETE SET NULL,

  fiscal_year INTEGER NOT NULL,
  period_start DATE,
  period_end DATE,
  statement_type TEXT NOT NULL DEFAULT 'annual_accounts',
  is_consolidated BOOLEAN NOT NULL DEFAULT false,
  original_currency TEXT,

  revenue_original_amount NUMERIC(20, 2),
  revenue_usd_cents BIGINT,
  operating_income_original_amount NUMERIC(20, 2),
  operating_income_usd_cents BIGINT,
  operating_profit_original_amount NUMERIC(20, 2),
  operating_profit_usd_cents BIGINT,
  profit_before_tax_original_amount NUMERIC(20, 2),
  profit_before_tax_usd_cents BIGINT,
  net_income_original_amount NUMERIC(20, 2),
  net_income_usd_cents BIGINT,

  total_assets_original_amount NUMERIC(20, 2),
  total_assets_usd_cents BIGINT,
  current_assets_original_amount NUMERIC(20, 2),
  current_assets_usd_cents BIGINT,
  fixed_assets_original_amount NUMERIC(20, 2),
  fixed_assets_usd_cents BIGINT,
  total_equity_original_amount NUMERIC(20, 2),
  total_equity_usd_cents BIGINT,
  total_liabilities_original_amount NUMERIC(20, 2),
  total_liabilities_usd_cents BIGINT,
  current_liabilities_original_amount NUMERIC(20, 2),
  current_liabilities_usd_cents BIGINT,
  long_term_liabilities_original_amount NUMERIC(20, 2),
  long_term_liabilities_usd_cents BIGINT,

  employee_count INTEGER,
  payroll_expenses_original_amount NUMERIC(20, 2),
  payroll_expenses_usd_cents BIGINT,

  operating_margin_percent NUMERIC(10, 4),
  net_margin_percent NUMERIC(10, 4),
  roe_percent NUMERIC(10, 4),
  roa_percent NUMERIC(10, 4),
  equity_ratio_percent NUMERIC(10, 4),
  current_ratio NUMERIC(10, 4),
  debt_to_equity_ratio NUMERIC(10, 4),

  fx_source TEXT,
  fx_rate_date DATE,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_url TEXT,
  facts JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_financial_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_financial_year CHECK (fiscal_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_brreg_source_financial_statement_type CHECK (
    statement_type IN ('annual_accounts', 'interim', 'unknown')
  ),
  CONSTRAINT chk_brreg_source_financial_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
  CONSTRAINT chk_brreg_source_financial_fx_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_brreg_source_financial_facts_object CHECK (jsonb_typeof(facts) = 'object'),
  CONSTRAINT chk_brreg_source_financial_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_financial_raw_object CHECK (jsonb_typeof(raw_financial_payload) = 'object'),
  CONSTRAINT chk_brreg_source_financial_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (company_id, fiscal_year, statement_type, is_consolidated)
);

CREATE INDEX idx_brreg_source_financial_company_year
  ON brreg_source.financial_statements(company_id, fiscal_year DESC);

CREATE INDEX idx_brreg_source_financial_revenue
  ON brreg_source.financial_statements(revenue_usd_cents DESC)
  WHERE revenue_usd_cents IS NOT NULL;

CREATE INDEX idx_brreg_source_financial_assets
  ON brreg_source.financial_statements(total_assets_usd_cents DESC)
  WHERE total_assets_usd_cents IS NOT NULL;

CREATE TABLE brreg_source.people_and_organizations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  holder_type TEXT NOT NULL,
  display_name TEXT NOT NULL,
  display_name_normalized TEXT NOT NULL,
  organization_number TEXT,
  birth_year INTEGER,
  country_iso2 TEXT,
  source_identifier TEXT,
  source TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_holders_type CHECK (holder_type IN ('person', 'organization', 'unknown')),
  CONSTRAINT chk_brreg_source_holders_birth_year CHECK (birth_year IS NULL OR birth_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_brreg_source_holders_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_brreg_source_holders_name
  ON brreg_source.people_and_organizations(display_name_normalized);

CREATE INDEX idx_brreg_source_holders_org
  ON brreg_source.people_and_organizations(organization_number)
  WHERE organization_number IS NOT NULL;

CREATE TABLE brreg_source.roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  holder_id UUID NOT NULL REFERENCES brreg_source.people_and_organizations(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES brreg_workflow.raw_records(id) ON DELETE SET NULL,

  role_code TEXT,
  role_label TEXT NOT NULL,
  role_label_en TEXT,
  role_group TEXT,
  role_group_en TEXT,
  started_at DATE,
  ended_at DATE,
  status TEXT NOT NULL DEFAULT 'active',
  source TEXT NOT NULL,

  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_role_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_roles_status CHECK (status IN ('active', 'ended', 'unknown')),
  CONSTRAINT chk_brreg_source_roles_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_roles_raw_object CHECK (jsonb_typeof(raw_role_payload) = 'object'),
  CONSTRAINT chk_brreg_source_roles_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (company_id, holder_id, role_code, started_at)
);

CREATE INDEX idx_brreg_source_roles_company
  ON brreg_source.roles(company_id, status);

CREATE INDEX idx_brreg_source_roles_holder
  ON brreg_source.roles(holder_id);

CREATE TABLE brreg_source.shareholdings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  holder_id UUID NOT NULL REFERENCES brreg_source.people_and_organizations(id) ON DELETE CASCADE,

  fiscal_year INTEGER,
  share_class TEXT,
  share_count NUMERIC(30, 6),
  ownership_percent NUMERIC(10, 6),
  voting_percent NUMERIC(10, 6),
  source TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',

  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_shareholding_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_shareholdings_year CHECK (fiscal_year IS NULL OR fiscal_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_brreg_source_shareholdings_percent CHECK (
    ownership_percent IS NULL OR ownership_percent BETWEEN 0 AND 100
  ),
  CONSTRAINT chk_brreg_source_shareholdings_voting CHECK (
    voting_percent IS NULL OR voting_percent BETWEEN 0 AND 100
  ),
  CONSTRAINT chk_brreg_source_shareholdings_status CHECK (status IN ('active', 'superseded', 'unknown')),
  CONSTRAINT chk_brreg_source_shareholdings_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_brreg_source_shareholdings_raw_object CHECK (jsonb_typeof(raw_shareholding_payload) = 'object'),
  CONSTRAINT chk_brreg_source_shareholdings_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (company_id, holder_id, fiscal_year, share_class)
);

CREATE INDEX idx_brreg_source_shareholdings_company
  ON brreg_source.shareholdings(company_id, fiscal_year DESC);

CREATE INDEX idx_brreg_source_shareholdings_holder
  ON brreg_source.shareholdings(holder_id);

CREATE TABLE brreg_source.action_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,

  action_type TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  target_key TEXT NOT NULL DEFAULT '',
  source_fingerprint TEXT NOT NULL,

  source_column TEXT,
  target_column TEXT,
  source_text TEXT,
  source_lang TEXT NOT NULL DEFAULT 'no',
  target_lang TEXT NOT NULL DEFAULT 'en',

  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  lease_until TIMESTAMPTZ,
  last_started_at TIMESTAMPTZ,
  last_finished_at TIMESTAMPTZ,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  model TEXT,
  prompt_version TEXT,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,

  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_action_type CHECK (
    action_type IN ('translate_field', 'discover_domains', 'convert_currency', 'build_suggestion')
  ),
  CONSTRAINT chk_brreg_source_action_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_brreg_source_action_attempt CHECK (attempt_count >= 0 AND max_attempts > 0),
  CONSTRAINT chk_brreg_source_action_result_object CHECK (jsonb_typeof(result) = 'object'),
  CONSTRAINT chk_brreg_source_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)
);

CREATE INDEX idx_brreg_source_action_queue
  ON brreg_source.action_tasks(action_type, status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX idx_brreg_source_action_company
  ON brreg_source.action_tasks(company_id, action_type, status);

CREATE OR REPLACE VIEW brreg_source.v_missing_translations AS
SELECT
  company.id AS company_id,
  'brreg_source.companies'::text AS source_table,
  company.id AS source_row_id,
  'short_description'::text AS source_column,
  'short_description_en'::text AS target_column,
  company.short_description AS source_text,
  encode(digest(company.short_description, 'sha256'), 'hex') AS source_text_hash,
  10::integer AS priority
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.short_description), '') IS NOT NULL
  AND nullif(btrim(company.short_description_en), '') IS NULL
UNION ALL
SELECT company.id, 'brreg_source.companies', company.id, 'description', 'description_en', company.description, encode(digest(company.description, 'sha256'), 'hex'), 10
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.description), '') IS NOT NULL
  AND nullif(btrim(company.description_en), '') IS NULL
UNION ALL
SELECT company.id, 'brreg_source.companies', company.id, 'registration_status_label', 'registration_status_label_en', company.registration_status_label, encode(digest(company.registration_status_label, 'sha256'), 'hex'), 20
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.registration_status_label), '') IS NOT NULL
  AND nullif(btrim(company.registration_status_label_en), '') IS NULL
UNION ALL
SELECT company.id, 'brreg_source.companies', company.id, 'organization_form_label', 'organization_form_label_en', company.organization_form_label, encode(digest(company.organization_form_label, 'sha256'), 'hex'), 20
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.organization_form_label), '') IS NOT NULL
  AND nullif(btrim(company.organization_form_label_en), '') IS NULL
UNION ALL
SELECT company.id, 'brreg_source.companies', company.id, 'response_class', 'response_class_en', company.response_class, encode(digest(company.response_class, 'sha256'), 'hex'), 30
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.response_class), '') IS NOT NULL
  AND nullif(btrim(company.response_class_en), '') IS NULL
UNION ALL
SELECT company.id, 'brreg_source.companies', company.id, 'activity_description', 'activity_description_en', company.activity_description, encode(digest(company.activity_description, 'sha256'), 'hex'), 10
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.activity_description), '') IS NOT NULL
  AND nullif(btrim(company.activity_description_en), '') IS NULL
UNION ALL
SELECT company.id, 'brreg_source.companies', company.id, 'statutory_purpose', 'statutory_purpose_en', company.statutory_purpose, encode(digest(company.statutory_purpose, 'sha256'), 'hex'), 10
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.statutory_purpose), '') IS NOT NULL
  AND nullif(btrim(company.statutory_purpose_en), '') IS NULL
UNION ALL
SELECT
  address.company_id,
  'brreg_source.addresses',
  address.id,
  'country',
  'country_en',
  address.country,
  encode(digest(address.country, 'sha256'), 'hex'),
  50
FROM brreg_source.addresses address
WHERE nullif(btrim(address.country), '') IS NOT NULL
  AND nullif(btrim(address.country_en), '') IS NULL
UNION ALL
SELECT
  industry.company_id,
  'brreg_source.industries',
  industry.id,
  'source_label',
  'source_label_en',
  industry.source_label,
  encode(digest(industry.source_label, 'sha256'), 'hex'),
  20
FROM brreg_source.industries industry
WHERE nullif(btrim(industry.source_label), '') IS NOT NULL
  AND nullif(btrim(industry.source_label_en), '') IS NULL
UNION ALL
SELECT
  website.company_id,
  'brreg_source.websites',
  website.id,
  'title',
  'title_en',
  website.title,
  encode(digest(website.title, 'sha256'), 'hex'),
  40
FROM brreg_source.websites website
WHERE nullif(btrim(website.title), '') IS NOT NULL
  AND nullif(btrim(website.title_en), '') IS NULL
UNION ALL
SELECT website.company_id, 'brreg_source.websites', website.id, 'description', 'description_en', website.description, encode(digest(website.description, 'sha256'), 'hex'), 40
FROM brreg_source.websites website
WHERE nullif(btrim(website.description), '') IS NOT NULL
  AND nullif(btrim(website.description_en), '') IS NULL
UNION ALL
SELECT
  contact.company_id,
  'brreg_source.contacts',
  contact.id,
  'label',
  'label_en',
  contact.label,
  encode(digest(contact.label, 'sha256'), 'hex'),
  50
FROM brreg_source.contacts contact
WHERE nullif(btrim(contact.label), '') IS NOT NULL
  AND nullif(btrim(contact.label_en), '') IS NULL
UNION ALL
SELECT
  capital.company_id,
  'brreg_source.capital',
  capital.id,
  'capital_type',
  'capital_type_en',
  capital.capital_type,
  encode(digest(capital.capital_type, 'sha256'), 'hex'),
  30
FROM brreg_source.capital capital
WHERE nullif(btrim(capital.capital_type), '') IS NOT NULL
  AND nullif(btrim(capital.capital_type_en), '') IS NULL
UNION ALL
SELECT
  role.company_id,
  'brreg_source.roles',
  role.id,
  'role_label',
  'role_label_en',
  role.role_label,
  encode(digest(role.role_label, 'sha256'), 'hex'),
  30
FROM brreg_source.roles role
WHERE nullif(btrim(role.role_label), '') IS NOT NULL
  AND nullif(btrim(role.role_label_en), '') IS NULL
UNION ALL
SELECT role.company_id, 'brreg_source.roles', role.id, 'role_group', 'role_group_en', role.role_group, encode(digest(role.role_group, 'sha256'), 'hex'), 30
FROM brreg_source.roles role
WHERE nullif(btrim(role.role_group), '') IS NOT NULL
  AND nullif(btrim(role.role_group_en), '') IS NULL;

CREATE OR REPLACE VIEW brreg_source.v_company_explorer AS
WITH primary_address AS (
  SELECT DISTINCT ON (company_id)
    company_id,
    city,
    municipality,
    municipality_number,
    county,
    postal_code,
    formatted_address,
    latitude,
    longitude,
    geocode_status
  FROM brreg_source.addresses
  ORDER BY company_id, (address_type = 'business') DESC, address_type
),
primary_industry AS (
  SELECT DISTINCT ON (company_id)
    company_id,
    source_code AS primary_industry_code,
    coalesce(source_label_en, source_label) AS primary_industry_label,
    mapped_nace_code AS primary_nace_code,
    coalesce(nace_title_en, nace_title) AS primary_nace_title
  FROM brreg_source.industries
  ORDER BY company_id, is_primary DESC, position ASC
),
latest_financial AS (
  SELECT DISTINCT ON (company_id)
    company_id,
    fiscal_year AS latest_financial_year,
    revenue_usd_cents AS latest_revenue_usd_cents,
    total_assets_usd_cents AS latest_total_assets_usd_cents,
    net_income_usd_cents AS latest_net_income_usd_cents
  FROM brreg_source.financial_statements
  ORDER BY company_id, fiscal_year DESC
),
website_counts AS (
  SELECT company_id, count(*)::bigint AS website_count
  FROM brreg_source.websites
  WHERE status = 'active'
  GROUP BY company_id
),
domain_counts AS (
  SELECT company_id, count(*)::bigint AS domain_count
  FROM brreg_source.domains
  WHERE status = 'active'
  GROUP BY company_id
),
contact_counts AS (
  SELECT company_id, count(*)::bigint AS contact_count
  FROM brreg_source.contacts
  WHERE status = 'active'
  GROUP BY company_id
),
translation_counts AS (
  SELECT company_id, count(*)::bigint AS translation_missing_count
  FROM brreg_source.v_missing_translations
  GROUP BY company_id
),
action_counts AS (
  SELECT
    company_id,
    count(*) FILTER (WHERE action_type = 'translate_field' AND status IN ('pending', 'failed_retryable'))::bigint AS translation_pending_count,
    count(*) FILTER (WHERE action_type = 'translate_field' AND status = 'running')::bigint AS translation_running_count,
    count(*) FILTER (WHERE action_type = 'translate_field' AND status = 'succeeded')::bigint AS translation_succeeded_count,
    count(*) FILTER (WHERE action_type = 'discover_domains' AND status IN ('pending', 'failed_retryable'))::bigint AS domain_pending_count,
    count(*) FILTER (WHERE action_type = 'discover_domains' AND status = 'running')::bigint AS domain_running_count,
    count(*) FILTER (WHERE action_type = 'discover_domains' AND status = 'succeeded')::bigint AS domain_succeeded_count
  FROM brreg_source.action_tasks
  GROUP BY company_id
)
SELECT
  company.id AS company_id,
  company.organization_number,
  company.organization_name,
  company.description_en,
  company.lifecycle_status,
  company.registration_status,
  company.organization_form_code,
  coalesce(company.organization_form_label_en, company.organization_form_label) AS organization_form_label,
  industry.primary_industry_code,
  industry.primary_industry_label,
  industry.primary_nace_code,
  industry.primary_nace_title,
  address.city,
  address.municipality,
  address.municipality_number,
  address.county,
  address.postal_code,
  address.formatted_address,
  address.latitude,
  address.longitude,
  address.geocode_status,
  company.employee_count,
  company.employee_band,
  coalesce(website_counts.website_count, 0) AS website_count,
  coalesce(domain_counts.domain_count, 0) AS domain_count,
  coalesce(contact_counts.contact_count, 0) AS contact_count,
  latest_financial.latest_financial_year,
  latest_financial.latest_revenue_usd_cents,
  latest_financial.latest_total_assets_usd_cents,
  latest_financial.latest_net_income_usd_cents,
  coalesce(translation_counts.translation_missing_count, 0) AS translation_missing_count,
  coalesce(action_counts.translation_pending_count, 0) AS translation_pending_count,
  coalesce(action_counts.translation_running_count, 0) AS translation_running_count,
  coalesce(action_counts.translation_succeeded_count, 0) AS translation_succeeded_count,
  coalesce(action_counts.domain_pending_count, 0) AS domain_pending_count,
  coalesce(action_counts.domain_running_count, 0) AS domain_running_count,
  coalesce(action_counts.domain_succeeded_count, 0) AS domain_succeeded_count,
  company.updated_at
FROM brreg_source.companies company
LEFT JOIN primary_address address ON address.company_id = company.id
LEFT JOIN primary_industry industry ON industry.company_id = company.id
LEFT JOIN latest_financial ON latest_financial.company_id = company.id
LEFT JOIN website_counts ON website_counts.company_id = company.id
LEFT JOIN domain_counts ON domain_counts.company_id = company.id
LEFT JOIN contact_counts ON contact_counts.company_id = company.id
LEFT JOIN translation_counts ON translation_counts.company_id = company.id
LEFT JOIN action_counts ON action_counts.company_id = company.id
WHERE company.row_status = 'active';

CREATE OR REPLACE VIEW brreg_source.v_company_detail AS
WITH address_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(addresses) - 'company_id' ORDER BY address_type) AS addresses
  FROM brreg_source.addresses
  GROUP BY company_id
),
industry_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(industries) - 'company_id' ORDER BY classification_type, position) AS industries
  FROM brreg_source.industries
  GROUP BY company_id
),
website_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(websites) - 'company_id' ORDER BY is_primary DESC, confidence DESC NULLS LAST, created_at DESC) AS websites
  FROM brreg_source.websites
  GROUP BY company_id
),
domain_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(domains) - 'company_id' ORDER BY is_primary DESC, confidence DESC, created_at DESC) AS domains
  FROM brreg_source.domains
  GROUP BY company_id
),
contact_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(contacts) - 'company_id' ORDER BY is_primary DESC, contact_type, created_at DESC) AS contacts
  FROM brreg_source.contacts
  GROUP BY company_id
),
financial_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(financial_statements) - 'company_id' ORDER BY fiscal_year DESC) AS financial_years
  FROM brreg_source.financial_statements
  GROUP BY company_id
),
role_rows AS (
  SELECT roles.company_id, jsonb_agg(
    (to_jsonb(roles) - 'company_id') ||
    jsonb_build_object('holder', to_jsonb(holders) - 'metadata' - 'created_at' - 'updated_at')
    ORDER BY roles.status, roles.role_group, roles.role_label
  ) AS roles
  FROM brreg_source.roles roles
  JOIN brreg_source.people_and_organizations holders ON holders.id = roles.holder_id
  GROUP BY roles.company_id
),
shareholding_rows AS (
  SELECT shareholdings.company_id, jsonb_agg(
    (to_jsonb(shareholdings) - 'company_id') ||
    jsonb_build_object('holder', to_jsonb(holders) - 'metadata' - 'created_at' - 'updated_at')
    ORDER BY shareholdings.fiscal_year DESC NULLS LAST, shareholdings.ownership_percent DESC NULLS LAST
  ) AS shareholdings
  FROM brreg_source.shareholdings shareholdings
  JOIN brreg_source.people_and_organizations holders ON holders.id = shareholdings.holder_id
  GROUP BY shareholdings.company_id
),
translation_rows AS (
  SELECT
    company_id,
    jsonb_build_object(
      'pending', count(*) FILTER (WHERE status = 'pending'),
      'running', count(*) FILTER (WHERE status = 'running'),
      'succeeded', count(*) FILTER (WHERE status = 'succeeded'),
      'failed_retryable', count(*) FILTER (WHERE status = 'failed_retryable'),
      'failed_terminal', count(*) FILTER (WHERE status = 'failed_terminal'),
      'skipped', count(*) FILTER (WHERE status = 'skipped')
    ) AS translation_status
  FROM brreg_source.action_tasks
  WHERE action_type = 'translate_field'
  GROUP BY company_id
)
SELECT
  company.*,
  coalesce(address_rows.addresses, '[]'::jsonb) AS addresses,
  coalesce(industry_rows.industries, '[]'::jsonb) AS industries,
  coalesce(website_rows.websites, '[]'::jsonb) AS websites,
  coalesce(domain_rows.domains, '[]'::jsonb) AS domains,
  coalesce(contact_rows.contacts, '[]'::jsonb) AS contacts,
  coalesce(financial_rows.financial_years, '[]'::jsonb) AS financial_years,
  coalesce(role_rows.roles, '[]'::jsonb) AS roles,
  coalesce(shareholding_rows.shareholdings, '[]'::jsonb) AS shareholdings,
  coalesce(translation_rows.translation_status, '{}'::jsonb) AS translation_status
FROM brreg_source.companies company
LEFT JOIN address_rows ON address_rows.company_id = company.id
LEFT JOIN industry_rows ON industry_rows.company_id = company.id
LEFT JOIN website_rows ON website_rows.company_id = company.id
LEFT JOIN domain_rows ON domain_rows.company_id = company.id
LEFT JOIN contact_rows ON contact_rows.company_id = company.id
LEFT JOIN financial_rows ON financial_rows.company_id = company.id
LEFT JOIN role_rows ON role_rows.company_id = company.id
LEFT JOIN shareholding_rows ON shareholding_rows.company_id = company.id
LEFT JOIN translation_rows ON translation_rows.company_id = company.id
WHERE company.row_status = 'active';

GRANT USAGE ON SCHEMA brreg_source TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA brreg_source TO corpscout_anon;
