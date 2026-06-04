CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS ariregister_source;

CREATE TABLE ariregister_source.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  registry_code TEXT NOT NULL,
  source_native_id TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'EE',
  legal_name TEXT NOT NULL,
  legal_name_normalized TEXT NOT NULL,
  legal_name_en TEXT,
  registration_status TEXT,
  registration_status_label TEXT,
  registration_status_label_en TEXT,
  lifecycle_status TEXT NOT NULL DEFAULT 'unknown',
  legal_form_code TEXT,
  legal_form_number INTEGER,
  legal_form_label TEXT,
  legal_form_label_en TEXT,
  legal_form_subtype TEXT,
  legal_form_subtype_label TEXT,
  legal_form_subtype_label_en TEXT,
  region_code INTEGER,
  region_label TEXT,
  region_label_en TEXT,
  region_label_long TEXT,
  region_label_long_en TEXT,
  active_label TEXT,
  active_label_en TEXT,
  first_registered_on DATE,
  deleted_on DATE,
  evks_registered_at DATE,
  has_missing_beneficial_owner_discrepancy_notice BOOLEAN,
  founded_without_contribution BOOLEAN,
  waived_form_requirements BOOLEAN,
  is_accounting_required BOOLEAN,
  reports_beneficial_owners BOOLEAN,
  is_active BOOLEAN,
  last_annual_report_year INTEGER,
  employee_count INTEGER,
  employee_count_source TEXT,
  employee_band TEXT,
  source_updated_at TIMESTAMPTZ,
  payload_hash TEXT NOT NULL,
  profile_version TEXT NOT NULL DEFAULT 'ariregister.source_profile.v1',
  row_status TEXT NOT NULL DEFAULT 'active',
  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_company_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,
  CONSTRAINT chk_ariregister_source_companies_status CHECK (row_status IN ('active', 'superseded')),
  CONSTRAINT chk_ariregister_source_companies_lifecycle CHECK (lifecycle_status IN ('active', 'inactive', 'deleted', 'unknown')),
  CONSTRAINT chk_ariregister_source_companies_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
  CONSTRAINT chk_ariregister_source_companies_payload_object CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_ariregister_source_companies_raw_object CHECK (jsonb_typeof(raw_company_payload) = 'object'),
  CONSTRAINT chk_ariregister_source_companies_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_ariregister_source_companies_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_ariregister_source_companies_active_registry
  ON ariregister_source.companies(registry_code)
  WHERE row_status = 'active';
CREATE INDEX idx_ariregister_source_companies_raw_record ON ariregister_source.companies(raw_record_id);
CREATE INDEX idx_ariregister_source_companies_name ON ariregister_source.companies(legal_name_normalized);
CREATE INDEX idx_ariregister_source_companies_status ON ariregister_source.companies(row_status, lifecycle_status, updated_at DESC);

CREATE TABLE ariregister_source.company_names (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  name TEXT NOT NULL,
  name_en TEXT,
  started_on DATE,
  ended_on DATE,
  raw_name_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_company_names_raw_object CHECK (jsonb_typeof(raw_name_payload) = 'object'),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.company_statuses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  status_code TEXT NOT NULL,
  status_label TEXT,
  status_label_en TEXT,
  started_on DATE,
  raw_status_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_company_statuses_raw_object CHECK (jsonb_typeof(raw_status_payload) = 'object'),
  UNIQUE (company_id, card_region, card_number, card_type, entry_number, status_code)
);

CREATE TABLE ariregister_source.legal_forms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  legal_form_code TEXT,
  legal_form_number INTEGER,
  legal_form_label TEXT,
  legal_form_label_en TEXT,
  legal_form_subtype TEXT,
  legal_form_subtype_label TEXT,
  legal_form_subtype_label_en TEXT,
  started_on DATE,
  ended_on DATE,
  raw_legal_form_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_legal_forms_raw_object CHECK (jsonb_typeof(raw_legal_form_payload) = 'object'),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.addresses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  address_type TEXT NOT NULL DEFAULT 'registered',
  country_code TEXT,
  country_label TEXT,
  country_label_en TEXT,
  ehak_code TEXT,
  ehak_name TEXT,
  ehak_name_en TEXT,
  street_text TEXT,
  street_text_en TEXT,
  postal_code TEXT,
  ads_oid TEXT,
  adr_id BIGINT,
  normalized_full_address TEXT,
  normalized_full_address_en TEXT,
  normalized_full_address_detail TEXT,
  code_address TEXT,
  adob_id TEXT,
  ads_type TEXT,
  started_on DATE,
  ended_on DATE,
  raw_address_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_addresses_raw_object CHECK (jsonb_typeof(raw_address_payload) = 'object'),
  CONSTRAINT chk_ariregister_source_addresses_type CHECK (address_type IN ('registered', 'report', 'other')),
  UNIQUE (company_id, source_entry_id)
);

CREATE INDEX idx_ariregister_source_addresses_company ON ariregister_source.addresses(company_id);
CREATE INDEX idx_ariregister_source_addresses_postal ON ariregister_source.addresses(postal_code) WHERE postal_code IS NOT NULL;
CREATE INDEX idx_ariregister_source_addresses_ehak ON ariregister_source.addresses(ehak_code) WHERE ehak_code IS NOT NULL;

CREATE TABLE ariregister_source.contacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES ariregister_workflow.raw_records(id) ON DELETE SET NULL,
  source_entry_id BIGINT,
  contact_type TEXT NOT NULL,
  contact_type_label TEXT,
  contact_type_label_en TEXT,
  value TEXT NOT NULL,
  normalized_value TEXT,
  source TEXT NOT NULL DEFAULT 'ariregister',
  status TEXT NOT NULL DEFAULT 'active',
  is_primary BOOLEAN NOT NULL DEFAULT false,
  ended_on DATE,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_contact_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_contacts_status CHECK (status IN ('active', 'removed', 'superseded')),
  CONSTRAINT chk_ariregister_source_contacts_raw_object CHECK (jsonb_typeof(raw_contact_payload) = 'object'),
  UNIQUE (company_id, contact_type, normalized_value)
);

CREATE INDEX idx_ariregister_source_contacts_company ON ariregister_source.contacts(company_id, contact_type, status);

CREATE TABLE ariregister_source.websites (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES ariregister_workflow.raw_records(id) ON DELETE SET NULL,
  contact_id UUID REFERENCES ariregister_source.contacts(id) ON DELETE SET NULL,
  url TEXT NOT NULL,
  normalized_url TEXT NOT NULL,
  host TEXT,
  path TEXT,
  website_type TEXT NOT NULL DEFAULT 'official_site',
  source TEXT NOT NULL DEFAULT 'ariregister_contact',
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT NOT NULL DEFAULT 90,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  title TEXT,
  title_en TEXT,
  description TEXT,
  description_en TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_websites_confidence CHECK (confidence BETWEEN 1 AND 100),
  UNIQUE (company_id, normalized_url)
);

CREATE INDEX idx_ariregister_source_websites_host ON ariregister_source.websites(host) WHERE host IS NOT NULL;

CREATE TABLE ariregister_source.domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES ariregister_workflow.raw_records(id) ON DELETE SET NULL,
  website_id UUID REFERENCES ariregister_source.websites(id) ON DELETE SET NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  registrable_domain TEXT NOT NULL,
  domain_type TEXT NOT NULL DEFAULT 'official',
  source TEXT NOT NULL DEFAULT 'ariregister_website',
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT NOT NULL DEFAULT 90,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  best_signal TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_domains_confidence CHECK (confidence BETWEEN 1 AND 100),
  UNIQUE (company_id, normalized_domain)
);

CREATE INDEX idx_ariregister_source_domains_domain ON ariregister_source.domains(normalized_domain);

CREATE TABLE ariregister_source.industries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  nace_code_id UUID REFERENCES nace_codes(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  classification_type TEXT NOT NULL DEFAULT 'declared_activity',
  source_field TEXT NOT NULL DEFAULT 'teatatud_tegevusalad',
  position SMALLINT NOT NULL DEFAULT 1,
  emtak_code TEXT NOT NULL,
  emtak_label TEXT,
  emtak_label_en TEXT,
  emtak_version INTEGER,
  emtak_version_label TEXT,
  emtak_version_label_en TEXT,
  nace_code TEXT,
  nace_revision TEXT,
  nace_title TEXT,
  nace_title_en TEXT,
  mapping_method TEXT,
  mapping_confidence REAL,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  started_on DATE,
  ended_on DATE,
  raw_industry_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_industries_position CHECK (position BETWEEN 1 AND 50),
  CONSTRAINT chk_ariregister_source_industries_confidence CHECK (mapping_confidence IS NULL OR mapping_confidence BETWEEN 0 AND 1),
  CONSTRAINT chk_ariregister_source_industries_raw_object CHECK (jsonb_typeof(raw_industry_payload) = 'object'),
  UNIQUE (company_id, classification_type, position)
);

CREATE INDEX idx_ariregister_source_industries_company ON ariregister_source.industries(company_id);
CREATE INDEX idx_ariregister_source_industries_emtak ON ariregister_source.industries(emtak_code);
CREATE INDEX idx_ariregister_source_industries_nace ON ariregister_source.industries(nace_code_id) WHERE nace_code_id IS NOT NULL;

CREATE TABLE ariregister_source.capital (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  capital_amount NUMERIC(20, 2),
  capital_currency TEXT,
  capital_currency_label TEXT,
  capital_currency_label_en TEXT,
  introduced_on DATE,
  ended_on DATE,
  raw_capital_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_capital_raw_object CHECK (jsonb_typeof(raw_capital_payload) = 'object'),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.financial_year_periods (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  period_start_month_day TEXT,
  period_end_month_day TEXT,
  started_on DATE,
  ended_on DATE,
  raw_period_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.annual_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  fiscal_year INTEGER,
  period_start DATE,
  period_end DATE,
  employee_count INTEGER,
  report_address TEXT,
  report_address_en TEXT,
  activity_emtak_code TEXT,
  activity_label TEXT,
  activity_label_en TEXT,
  activity_version TEXT,
  activity_version_label TEXT,
  activity_version_label_en TEXT,
  activity_nace_code TEXT,
  raw_report_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_reports_year CHECK (fiscal_year IS NULL OR fiscal_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_ariregister_source_reports_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
  UNIQUE (company_id, source_entry_id)
);

CREATE INDEX idx_ariregister_source_reports_company_year ON ariregister_source.annual_reports(company_id, fiscal_year DESC);

CREATE TABLE ariregister_source.articles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  confirmed_on DATE,
  changed_on DATE,
  explanation TEXT,
  explanation_en TEXT,
  contains_special_rights BOOLEAN,
  started_on DATE,
  ended_on DATE,
  raw_articles_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.registry_notes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  column_number INTEGER,
  note_type TEXT,
  note_type_label TEXT,
  note_type_label_en TEXT,
  note_text TEXT,
  note_text_en TEXT,
  started_on DATE,
  ended_on DATE,
  raw_note_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.action_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  target_key TEXT NOT NULL DEFAULT '',
  source_fingerprint TEXT NOT NULL,
  source_column TEXT,
  target_column TEXT,
  source_text TEXT,
  source_lang TEXT NOT NULL DEFAULT 'et',
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
  CONSTRAINT chk_ariregister_source_action_type CHECK (action_type IN ('translate_field', 'discover_domains', 'build_suggestion')),
  CONSTRAINT chk_ariregister_source_action_status CHECK (status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')),
  CONSTRAINT chk_ariregister_source_action_attempt CHECK (attempt_count >= 0 AND max_attempts > 0),
  CONSTRAINT chk_ariregister_source_action_result_object CHECK (jsonb_typeof(result) = 'object'),
  CONSTRAINT chk_ariregister_source_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)
);

CREATE INDEX idx_ariregister_source_action_queue
  ON ariregister_source.action_tasks(action_type, status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');
CREATE INDEX idx_ariregister_source_action_company
  ON ariregister_source.action_tasks(company_id, action_type, status);

CREATE OR REPLACE VIEW ariregister_source.v_missing_translations AS
SELECT company.id AS company_id, 'ariregister_source.companies'::text AS source_table, company.id AS source_row_id, 'legal_name'::text AS source_column, 'legal_name_en'::text AS target_column, company.legal_name AS source_text, encode(digest(company.legal_name, 'sha256'), 'hex') AS source_text_hash, 90::integer AS priority
FROM ariregister_source.companies company
WHERE company.row_status = 'active' AND nullif(btrim(company.legal_name), '') IS NOT NULL AND nullif(btrim(company.legal_name_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'registration_status_label', 'registration_status_label_en', company.registration_status_label, encode(digest(company.registration_status_label, 'sha256'), 'hex'), 20 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.registration_status_label), '') IS NOT NULL AND nullif(btrim(company.registration_status_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'legal_form_label', 'legal_form_label_en', company.legal_form_label, encode(digest(company.legal_form_label, 'sha256'), 'hex'), 20 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.legal_form_label), '') IS NOT NULL AND nullif(btrim(company.legal_form_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'legal_form_subtype_label', 'legal_form_subtype_label_en', company.legal_form_subtype_label, encode(digest(company.legal_form_subtype_label, 'sha256'), 'hex'), 30 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.legal_form_subtype_label), '') IS NOT NULL AND nullif(btrim(company.legal_form_subtype_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'region_label', 'region_label_en', company.region_label, encode(digest(company.region_label, 'sha256'), 'hex'), 40 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.region_label), '') IS NOT NULL AND nullif(btrim(company.region_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'region_label_long', 'region_label_long_en', company.region_label_long, encode(digest(company.region_label_long, 'sha256'), 'hex'), 40 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.region_label_long), '') IS NOT NULL AND nullif(btrim(company.region_label_long_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'active_label', 'active_label_en', company.active_label, encode(digest(company.active_label, 'sha256'), 'hex'), 30 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.active_label), '') IS NOT NULL AND nullif(btrim(company.active_label_en), '') IS NULL
UNION ALL SELECT status.company_id, 'ariregister_source.company_statuses', status.id, 'status_label', 'status_label_en', status.status_label, encode(digest(status.status_label, 'sha256'), 'hex'), 30 FROM ariregister_source.company_statuses status WHERE nullif(btrim(status.status_label), '') IS NOT NULL AND nullif(btrim(status.status_label_en), '') IS NULL
UNION ALL SELECT form.company_id, 'ariregister_source.legal_forms', form.id, 'legal_form_label', 'legal_form_label_en', form.legal_form_label, encode(digest(form.legal_form_label, 'sha256'), 'hex'), 30 FROM ariregister_source.legal_forms form WHERE nullif(btrim(form.legal_form_label), '') IS NOT NULL AND nullif(btrim(form.legal_form_label_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'country_label', 'country_label_en', address.country_label, encode(digest(address.country_label, 'sha256'), 'hex'), 50 FROM ariregister_source.addresses address WHERE nullif(btrim(address.country_label), '') IS NOT NULL AND nullif(btrim(address.country_label_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'ehak_name', 'ehak_name_en', address.ehak_name, encode(digest(address.ehak_name, 'sha256'), 'hex'), 50 FROM ariregister_source.addresses address WHERE nullif(btrim(address.ehak_name), '') IS NOT NULL AND nullif(btrim(address.ehak_name_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'street_text', 'street_text_en', address.street_text, encode(digest(address.street_text, 'sha256'), 'hex'), 80 FROM ariregister_source.addresses address WHERE nullif(btrim(address.street_text), '') IS NOT NULL AND nullif(btrim(address.street_text_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'normalized_full_address', 'normalized_full_address_en', address.normalized_full_address, encode(digest(address.normalized_full_address, 'sha256'), 'hex'), 80 FROM ariregister_source.addresses address WHERE nullif(btrim(address.normalized_full_address), '') IS NOT NULL AND nullif(btrim(address.normalized_full_address_en), '') IS NULL
UNION ALL SELECT contact.company_id, 'ariregister_source.contacts', contact.id, 'contact_type_label', 'contact_type_label_en', contact.contact_type_label, encode(digest(contact.contact_type_label, 'sha256'), 'hex'), 50 FROM ariregister_source.contacts contact WHERE nullif(btrim(contact.contact_type_label), '') IS NOT NULL AND nullif(btrim(contact.contact_type_label_en), '') IS NULL
UNION ALL SELECT industry.company_id, 'ariregister_source.industries', industry.id, 'emtak_label', 'emtak_label_en', industry.emtak_label, encode(digest(industry.emtak_label, 'sha256'), 'hex'), 20 FROM ariregister_source.industries industry WHERE nullif(btrim(industry.emtak_label), '') IS NOT NULL AND nullif(btrim(industry.emtak_label_en), '') IS NULL
UNION ALL SELECT industry.company_id, 'ariregister_source.industries', industry.id, 'emtak_version_label', 'emtak_version_label_en', industry.emtak_version_label, encode(digest(industry.emtak_version_label, 'sha256'), 'hex'), 40 FROM ariregister_source.industries industry WHERE nullif(btrim(industry.emtak_version_label), '') IS NOT NULL AND nullif(btrim(industry.emtak_version_label_en), '') IS NULL
UNION ALL SELECT capital.company_id, 'ariregister_source.capital', capital.id, 'capital_currency_label', 'capital_currency_label_en', capital.capital_currency_label, encode(digest(capital.capital_currency_label, 'sha256'), 'hex'), 50 FROM ariregister_source.capital capital WHERE nullif(btrim(capital.capital_currency_label), '') IS NOT NULL AND nullif(btrim(capital.capital_currency_label_en), '') IS NULL
UNION ALL SELECT report.company_id, 'ariregister_source.annual_reports', report.id, 'report_address', 'report_address_en', report.report_address, encode(digest(report.report_address, 'sha256'), 'hex'), 80 FROM ariregister_source.annual_reports report WHERE nullif(btrim(report.report_address), '') IS NOT NULL AND nullif(btrim(report.report_address_en), '') IS NULL
UNION ALL SELECT report.company_id, 'ariregister_source.annual_reports', report.id, 'activity_label', 'activity_label_en', report.activity_label, encode(digest(report.activity_label, 'sha256'), 'hex'), 20 FROM ariregister_source.annual_reports report WHERE nullif(btrim(report.activity_label), '') IS NOT NULL AND nullif(btrim(report.activity_label_en), '') IS NULL
UNION ALL SELECT report.company_id, 'ariregister_source.annual_reports', report.id, 'activity_version_label', 'activity_version_label_en', report.activity_version_label, encode(digest(report.activity_version_label, 'sha256'), 'hex'), 40 FROM ariregister_source.annual_reports report WHERE nullif(btrim(report.activity_version_label), '') IS NOT NULL AND nullif(btrim(report.activity_version_label_en), '') IS NULL
UNION ALL SELECT article.company_id, 'ariregister_source.articles', article.id, 'explanation', 'explanation_en', article.explanation, encode(digest(article.explanation, 'sha256'), 'hex'), 70 FROM ariregister_source.articles article WHERE nullif(btrim(article.explanation), '') IS NOT NULL AND nullif(btrim(article.explanation_en), '') IS NULL
UNION ALL SELECT note.company_id, 'ariregister_source.registry_notes', note.id, 'note_type_label', 'note_type_label_en', note.note_type_label, encode(digest(note.note_type_label, 'sha256'), 'hex'), 60 FROM ariregister_source.registry_notes note WHERE nullif(btrim(note.note_type_label), '') IS NOT NULL AND nullif(btrim(note.note_type_label_en), '') IS NULL
UNION ALL SELECT note.company_id, 'ariregister_source.registry_notes', note.id, 'note_text', 'note_text_en', note.note_text, encode(digest(note.note_text, 'sha256'), 'hex'), 60 FROM ariregister_source.registry_notes note WHERE nullif(btrim(note.note_text), '') IS NOT NULL AND nullif(btrim(note.note_text_en), '') IS NULL;

CREATE MATERIALIZED VIEW ariregister_source.mv_company_explorer AS
WITH primary_address AS (
  SELECT DISTINCT ON (company_id) company_id, ehak_name AS city_or_area, postal_code, normalized_full_address
  FROM ariregister_source.addresses
  ORDER BY company_id, started_on DESC NULLS LAST, created_at DESC
),
primary_industry AS (
  SELECT DISTINCT ON (company_id) company_id, emtak_code AS primary_industry_code, coalesce(emtak_label_en, emtak_label) AS primary_industry_label, nace_code AS primary_nace_code, coalesce(nace_title_en, nace_title) AS primary_nace_title
  FROM ariregister_source.industries
  ORDER BY company_id, is_primary DESC, position ASC
),
latest_report AS (
  SELECT DISTINCT ON (company_id) company_id, fiscal_year AS latest_financial_year, employee_count
  FROM ariregister_source.annual_reports
  ORDER BY company_id, fiscal_year DESC NULLS LAST, period_end DESC NULLS LAST
),
website_counts AS (
  SELECT company_id, count(*)::bigint AS website_count FROM ariregister_source.websites WHERE status = 'active' GROUP BY company_id
),
domain_counts AS (
  SELECT company_id, count(*)::bigint AS domain_count FROM ariregister_source.domains WHERE status = 'active' GROUP BY company_id
),
contact_counts AS (
  SELECT company_id, count(*)::bigint AS contact_count FROM ariregister_source.contacts WHERE status = 'active' GROUP BY company_id
),
translation_counts AS (
  SELECT company_id, count(*)::bigint AS translation_missing_count FROM ariregister_source.v_missing_translations GROUP BY company_id
)
SELECT
  company.id AS company_id,
  company.registry_code,
  company.legal_name,
  coalesce(company.legal_form_label_en, company.legal_form_label) AS legal_form_label,
  company.lifecycle_status,
  company.registration_status,
  coalesce(company.registration_status_label_en, company.registration_status_label) AS registration_status_label,
  industry.primary_industry_code,
  industry.primary_industry_label,
  industry.primary_nace_code,
  industry.primary_nace_title,
  address.city_or_area,
  address.postal_code,
  address.normalized_full_address,
  coalesce(report.employee_count, company.employee_count) AS employee_count,
  report.latest_financial_year,
  coalesce(website_counts.website_count, 0) AS website_count,
  coalesce(domain_counts.domain_count, 0) AS domain_count,
  coalesce(contact_counts.contact_count, 0) AS contact_count,
  coalesce(translation_counts.translation_missing_count, 0) AS translation_missing_count,
  company.updated_at
FROM ariregister_source.companies company
LEFT JOIN primary_address address ON address.company_id = company.id
LEFT JOIN primary_industry industry ON industry.company_id = company.id
LEFT JOIN latest_report report ON report.company_id = company.id
LEFT JOIN website_counts ON website_counts.company_id = company.id
LEFT JOIN domain_counts ON domain_counts.company_id = company.id
LEFT JOIN contact_counts ON contact_counts.company_id = company.id
LEFT JOIN translation_counts ON translation_counts.company_id = company.id
WHERE company.row_status = 'active';

CREATE UNIQUE INDEX uq_ariregister_source_mv_company_explorer_company ON ariregister_source.mv_company_explorer(company_id);
CREATE INDEX idx_ariregister_source_mv_company_explorer_registry ON ariregister_source.mv_company_explorer(registry_code);
CREATE INDEX idx_ariregister_source_mv_company_explorer_name ON ariregister_source.mv_company_explorer(legal_name);
CREATE INDEX idx_ariregister_source_mv_company_explorer_translation ON ariregister_source.mv_company_explorer(translation_missing_count) WHERE translation_missing_count > 0;

CREATE OR REPLACE VIEW ariregister_source.v_company_detail AS
WITH company_names AS (
  SELECT company_id, jsonb_agg(to_jsonb(company_names) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS names FROM ariregister_source.company_names GROUP BY company_id
),
statuses AS (
  SELECT company_id, jsonb_agg(to_jsonb(company_statuses) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS statuses FROM ariregister_source.company_statuses GROUP BY company_id
),
legal_forms AS (
  SELECT company_id, jsonb_agg(to_jsonb(legal_forms) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS legal_forms FROM ariregister_source.legal_forms GROUP BY company_id
),
addresses AS (
  SELECT company_id, jsonb_agg(to_jsonb(addresses) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS addresses FROM ariregister_source.addresses GROUP BY company_id
),
contacts AS (
  SELECT company_id, jsonb_agg(to_jsonb(contacts) - 'company_id' ORDER BY is_primary DESC, contact_type, created_at DESC) AS contacts FROM ariregister_source.contacts GROUP BY company_id
),
websites AS (
  SELECT company_id, jsonb_agg(to_jsonb(websites) - 'company_id' ORDER BY is_primary DESC, confidence DESC, created_at DESC) AS websites FROM ariregister_source.websites GROUP BY company_id
),
domains AS (
  SELECT company_id, jsonb_agg(to_jsonb(domains) - 'company_id' ORDER BY is_primary DESC, confidence DESC, created_at DESC) AS domains FROM ariregister_source.domains GROUP BY company_id
),
industries AS (
  SELECT company_id, jsonb_agg(to_jsonb(industries) - 'company_id' ORDER BY is_primary DESC, position) AS industries FROM ariregister_source.industries GROUP BY company_id
),
capital AS (
  SELECT company_id, jsonb_agg(to_jsonb(capital) - 'company_id' ORDER BY introduced_on DESC NULLS LAST) AS capital FROM ariregister_source.capital GROUP BY company_id
),
annual_reports AS (
  SELECT company_id, jsonb_agg(to_jsonb(annual_reports) - 'company_id' ORDER BY fiscal_year DESC NULLS LAST) AS annual_reports FROM ariregister_source.annual_reports GROUP BY company_id
),
articles AS (
  SELECT company_id, jsonb_agg(to_jsonb(articles) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS articles FROM ariregister_source.articles GROUP BY company_id
),
registry_notes AS (
  SELECT company_id, jsonb_agg(to_jsonb(registry_notes) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS registry_notes FROM ariregister_source.registry_notes GROUP BY company_id
)
SELECT
  company.*,
  coalesce(company_names.names, '[]'::jsonb) AS names,
  coalesce(statuses.statuses, '[]'::jsonb) AS statuses,
  coalesce(legal_forms.legal_forms, '[]'::jsonb) AS legal_forms,
  coalesce(addresses.addresses, '[]'::jsonb) AS addresses,
  coalesce(contacts.contacts, '[]'::jsonb) AS contacts,
  coalesce(websites.websites, '[]'::jsonb) AS websites,
  coalesce(domains.domains, '[]'::jsonb) AS domains,
  coalesce(industries.industries, '[]'::jsonb) AS industries,
  coalesce(capital.capital, '[]'::jsonb) AS capital,
  coalesce(annual_reports.annual_reports, '[]'::jsonb) AS annual_reports,
  coalesce(articles.articles, '[]'::jsonb) AS articles,
  coalesce(registry_notes.registry_notes, '[]'::jsonb) AS registry_notes
FROM ariregister_source.companies company
LEFT JOIN company_names ON company_names.company_id = company.id
LEFT JOIN statuses ON statuses.company_id = company.id
LEFT JOIN legal_forms ON legal_forms.company_id = company.id
LEFT JOIN addresses ON addresses.company_id = company.id
LEFT JOIN contacts ON contacts.company_id = company.id
LEFT JOIN websites ON websites.company_id = company.id
LEFT JOIN domains ON domains.company_id = company.id
LEFT JOIN industries ON industries.company_id = company.id
LEFT JOIN capital ON capital.company_id = company.id
LEFT JOIN annual_reports ON annual_reports.company_id = company.id
LEFT JOIN articles ON articles.company_id = company.id
LEFT JOIN registry_notes ON registry_notes.company_id = company.id
WHERE company.row_status = 'active';
