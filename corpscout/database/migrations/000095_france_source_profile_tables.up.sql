CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS france_source;

CREATE TABLE france_source.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_legal_unit_id UUID NOT NULL REFERENCES france_workflow.raw_legal_units(id) ON DELETE RESTRICT,

  siren TEXT NOT NULL,
  source_native_id TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'FR',

  organization_name TEXT NOT NULL,
  organization_name_normalized TEXT NOT NULL,
  organization_name_en TEXT,
  acronym TEXT,
  short_description TEXT,
  short_description_en TEXT,
  description TEXT,
  description_en TEXT,

  diffusion_status TEXT,
  is_partial_diffusion BOOLEAN NOT NULL DEFAULT false,
  registration_status TEXT,
  registration_status_label TEXT,
  registration_status_label_en TEXT,
  lifecycle_status TEXT NOT NULL DEFAULT 'unknown',
  is_purged BOOLEAN,

  legal_form_code TEXT,
  legal_form_label TEXT,
  legal_form_label_en TEXT,
  company_category TEXT,
  company_category_label TEXT,
  company_category_label_en TEXT,
  company_category_year TEXT,

  primary_activity_code TEXT,
  primary_activity_nomenclature TEXT,
  primary_activity_naf25_code TEXT,
  primary_activity_label TEXT,
  primary_activity_label_en TEXT,

  employee_band TEXT,
  employee_band_year TEXT,
  founded_date DATE,
  period_started_at DATE,
  period_count INTEGER,
  headquarters_nic TEXT,
  headquarters_siret TEXT,
  association_identifier TEXT,

  social_solidarity BOOLEAN,
  mission_company BOOLEAN,
  is_employer BOOLEAN,

  source_updated_at TIMESTAMPTZ,
  payload_hash TEXT NOT NULL,
  profile_version TEXT NOT NULL DEFAULT 'france.source_profile.v1',
  row_status TEXT NOT NULL DEFAULT 'active',

  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_company_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,

  CONSTRAINT chk_france_source_companies_siren CHECK (siren ~ '^[0-9]{9}$'),
  CONSTRAINT chk_france_source_companies_source_native CHECK (source_native_id = siren),
  CONSTRAINT chk_france_source_companies_status CHECK (row_status IN ('active', 'superseded')),
  CONSTRAINT chk_france_source_companies_lifecycle CHECK (
    lifecycle_status IN ('active', 'inactive', 'unknown')
  ),
  CONSTRAINT chk_france_source_companies_payload_object CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_france_source_companies_raw_object CHECK (jsonb_typeof(raw_company_payload) = 'object'),
  CONSTRAINT chk_france_source_companies_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_companies_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_france_source_companies_active_siren
  ON france_source.companies (siren)
  WHERE row_status = 'active';

CREATE INDEX idx_france_source_companies_raw_legal_unit
  ON france_source.companies (raw_legal_unit_id);

CREATE INDEX idx_france_source_companies_name
  ON france_source.companies (organization_name_normalized);

CREATE INDEX idx_france_source_companies_status
  ON france_source.companies (row_status, lifecycle_status, updated_at DESC);

CREATE TABLE france_source.establishments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,
  raw_establishment_id UUID NOT NULL REFERENCES france_workflow.raw_establishments(id) ON DELETE RESTRICT,

  siren TEXT NOT NULL,
  siret TEXT NOT NULL,
  nic TEXT NOT NULL,
  source_native_id TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'FR',

  establishment_name TEXT,
  establishment_name_normalized TEXT,
  trade_name TEXT,
  usual_name TEXT,
  is_headquarters BOOLEAN NOT NULL DEFAULT false,

  diffusion_status TEXT,
  is_partial_diffusion BOOLEAN NOT NULL DEFAULT false,
  registration_status TEXT,
  registration_status_label TEXT,
  registration_status_label_en TEXT,
  lifecycle_status TEXT NOT NULL DEFAULT 'unknown',

  primary_activity_code TEXT,
  primary_activity_nomenclature TEXT,
  primary_activity_naf25_code TEXT,
  primary_activity_label TEXT,
  primary_activity_label_en TEXT,
  crafts_registry_activity TEXT,

  employee_band TEXT,
  employee_band_year TEXT,
  founded_date DATE,
  period_started_at DATE,
  period_count INTEGER,
  is_employer BOOLEAN,

  source_updated_at TIMESTAMPTZ,
  payload_hash TEXT NOT NULL,
  row_status TEXT NOT NULL DEFAULT 'active',

  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_establishment_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,

  CONSTRAINT chk_france_source_establishments_siren CHECK (siren ~ '^[0-9]{9}$'),
  CONSTRAINT chk_france_source_establishments_siret CHECK (siret ~ '^[0-9]{14}$'),
  CONSTRAINT chk_france_source_establishments_nic CHECK (nic ~ '^[0-9]{5}$'),
  CONSTRAINT chk_france_source_establishments_source_native CHECK (source_native_id = siret),
  CONSTRAINT chk_france_source_establishments_status CHECK (row_status IN ('active', 'superseded')),
  CONSTRAINT chk_france_source_establishments_lifecycle CHECK (
    lifecycle_status IN ('active', 'inactive', 'unknown')
  ),
  CONSTRAINT chk_france_source_establishments_payload_object CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_france_source_establishments_raw_object CHECK (jsonb_typeof(raw_establishment_payload) = 'object'),
  CONSTRAINT chk_france_source_establishments_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_establishments_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_france_source_establishments_active_siret
  ON france_source.establishments (siret)
  WHERE row_status = 'active';

CREATE UNIQUE INDEX uq_france_source_establishments_active_headquarters
  ON france_source.establishments (company_id)
  WHERE row_status = 'active' AND is_headquarters;

CREATE INDEX idx_france_source_establishments_company
  ON france_source.establishments (company_id, is_headquarters DESC);

CREATE INDEX idx_france_source_establishments_raw
  ON france_source.establishments (raw_establishment_id);

CREATE TABLE france_source.addresses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,
  establishment_id UUID NOT NULL REFERENCES france_source.establishments(id) ON DELETE CASCADE,
  raw_establishment_id UUID NOT NULL REFERENCES france_workflow.raw_establishments(id) ON DELETE RESTRICT,

  address_type TEXT NOT NULL,
  address_rank SMALLINT NOT NULL DEFAULT 1,
  complement TEXT,
  street_number TEXT,
  street_number_suffix TEXT,
  last_street_number TEXT,
  last_street_number_suffix TEXT,
  street_type TEXT,
  street_name TEXT,
  postal_code TEXT,
  city TEXT,
  foreign_city TEXT,
  special_distribution TEXT,
  commune_code TEXT,
  cedex_code TEXT,
  cedex_label TEXT,
  foreign_country_code TEXT,
  foreign_country TEXT,
  foreign_country_en TEXT,
  address_identifier TEXT,
  formatted_address TEXT,

  lambert_x TEXT,
  lambert_y TEXT,
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

  CONSTRAINT chk_france_source_addresses_type CHECK (
    address_type IN ('headquarters', 'establishment', 'secondary')
  ),
  CONSTRAINT chk_france_source_addresses_rank CHECK (address_rank IN (1, 2)),
  CONSTRAINT chk_france_source_addresses_geocode CHECK (
    geocode_status IN ('not_attempted', 'queued', 'running', 'succeeded', 'failed', 'not_found', 'ambiguous')
  ),
  CONSTRAINT chk_france_source_addresses_coordinates CHECK (
    (latitude IS NULL AND longitude IS NULL) OR
    (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
  ),
  CONSTRAINT chk_france_source_addresses_geocode_confidence CHECK (
    geocode_confidence IS NULL OR geocode_confidence BETWEEN 1 AND 100
  ),
  CONSTRAINT chk_france_source_addresses_geocode_payload_object CHECK (jsonb_typeof(geocode_payload) = 'object'),
  CONSTRAINT chk_france_source_addresses_raw_object CHECK (jsonb_typeof(raw_address_payload) = 'object'),
  CONSTRAINT chk_france_source_addresses_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_addresses_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (establishment_id, address_rank)
);

CREATE INDEX idx_france_source_addresses_company
  ON france_source.addresses (company_id);

CREATE INDEX idx_france_source_addresses_establishment
  ON france_source.addresses (establishment_id);

CREATE INDEX idx_france_source_addresses_postal_code
  ON france_source.addresses (postal_code)
  WHERE postal_code IS NOT NULL;

CREATE INDEX idx_france_source_addresses_commune
  ON france_source.addresses (commune_code)
  WHERE commune_code IS NOT NULL;

CREATE INDEX idx_france_source_addresses_geocode_queue
  ON france_source.addresses (geocode_status, updated_at)
  WHERE geocode_status IN ('not_attempted', 'queued', 'failed', 'ambiguous');

CREATE TABLE france_source.industries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,
  establishment_id UUID REFERENCES france_source.establishments(id) ON DELETE CASCADE,
  raw_legal_unit_id UUID REFERENCES france_workflow.raw_legal_units(id) ON DELETE SET NULL,
  raw_establishment_id UUID REFERENCES france_workflow.raw_establishments(id) ON DELETE SET NULL,
  nace_code_id UUID REFERENCES nace_codes(id) ON DELETE RESTRICT,

  classification_type TEXT NOT NULL DEFAULT 'industry',
  source_field TEXT NOT NULL,
  position SMALLINT NOT NULL DEFAULT 1,
  source_code TEXT NOT NULL,
  source_nomenclature TEXT,
  source_label TEXT,
  source_label_en TEXT,
  naf25_code TEXT,

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

  CONSTRAINT chk_france_source_industries_type CHECK (
    classification_type IN ('industry')
  ),
  CONSTRAINT chk_france_source_industries_position CHECK (position BETWEEN 1 AND 10),
  CONSTRAINT chk_france_source_industries_confidence CHECK (
    mapping_confidence IS NULL OR mapping_confidence BETWEEN 0 AND 1
  ),
  CONSTRAINT chk_france_source_industries_raw_object CHECK (jsonb_typeof(raw_industry_payload) = 'object'),
  CONSTRAINT chk_france_source_industries_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_industries_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_france_source_industries_company
  ON france_source.industries (company_id);

CREATE INDEX idx_france_source_industries_establishment
  ON france_source.industries (establishment_id)
  WHERE establishment_id IS NOT NULL;

CREATE INDEX idx_france_source_industries_source_code
  ON france_source.industries (source_nomenclature, source_code);

CREATE INDEX idx_france_source_industries_nace
  ON france_source.industries (nace_code_id)
  WHERE nace_code_id IS NOT NULL;

CREATE TABLE france_source.websites (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,
  establishment_id UUID REFERENCES france_source.establishments(id) ON DELETE SET NULL,

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

  CONSTRAINT chk_france_source_websites_type CHECK (
    website_type IN ('official_site', 'social_profile', 'marketplace', 'directory_profile', 'contact_page', 'other', 'unknown')
  ),
  CONSTRAINT chk_france_source_websites_source CHECK (
    source IN ('france', 'manual', 'domain_discovery', 'email', 'imported', 'other')
  ),
  CONSTRAINT chk_france_source_websites_status CHECK (
    status IN ('active', 'rejected', 'removed', 'superseded')
  ),
  CONSTRAINT chk_france_source_websites_confidence CHECK (confidence IS NULL OR confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_france_source_websites_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_websites_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_france_source_websites_active_url
  ON france_source.websites (company_id, normalized_url)
  WHERE status = 'active';

CREATE UNIQUE INDEX uq_france_source_websites_primary
  ON france_source.websites (company_id)
  WHERE status = 'active' AND is_primary;

CREATE INDEX idx_france_source_websites_host
  ON france_source.websites (host)
  WHERE host IS NOT NULL;

CREATE TABLE france_source.domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,
  establishment_id UUID REFERENCES france_source.establishments(id) ON DELETE SET NULL,
  website_id UUID REFERENCES france_source.websites(id) ON DELETE SET NULL,

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

  CONSTRAINT chk_france_source_domains_type CHECK (
    domain_type IN ('official', 'related', 'email_domain', 'infrastructure', 'unknown')
  ),
  CONSTRAINT chk_france_source_domains_source CHECK (
    source IN ('france', 'manual', 'domain_discovery', 'email', 'imported', 'other')
  ),
  CONSTRAINT chk_france_source_domains_status CHECK (
    status IN ('active', 'rejected', 'removed', 'superseded')
  ),
  CONSTRAINT chk_france_source_domains_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_france_source_domains_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_domains_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_france_source_domains_active
  ON france_source.domains (company_id, normalized_domain)
  WHERE status = 'active';

CREATE UNIQUE INDEX uq_france_source_domains_primary
  ON france_source.domains (company_id)
  WHERE status = 'active' AND is_primary;

CREATE INDEX idx_france_source_domains_domain
  ON france_source.domains (normalized_domain);

CREATE TABLE france_source.contacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,
  establishment_id UUID REFERENCES france_source.establishments(id) ON DELETE SET NULL,
  website_id UUID REFERENCES france_source.websites(id) ON DELETE SET NULL,

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

  CONSTRAINT chk_france_source_contacts_type CHECK (
    contact_type IN ('phone', 'mobile', 'email', 'contact_form', 'fax', 'social_handle', 'other')
  ),
  CONSTRAINT chk_france_source_contacts_source CHECK (
    source IN ('france', 'manual', 'website', 'domain_discovery', 'imported', 'other')
  ),
  CONSTRAINT chk_france_source_contacts_status CHECK (
    status IN ('active', 'rejected', 'removed', 'superseded')
  ),
  CONSTRAINT chk_france_source_contacts_confidence CHECK (confidence IS NULL OR confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_france_source_contacts_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_france_source_contacts_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_france_source_contacts_active
  ON france_source.contacts (company_id, contact_type, normalized_value)
  WHERE status = 'active' AND normalized_value IS NOT NULL;

CREATE INDEX idx_france_source_contacts_company
  ON france_source.contacts (company_id, contact_type, status);

CREATE TABLE france_source.action_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES france_source.companies(id) ON DELETE CASCADE,

  action_type TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  target_key TEXT NOT NULL DEFAULT '',
  source_fingerprint TEXT NOT NULL,

  source_column TEXT,
  target_column TEXT,
  source_text TEXT,
  source_lang TEXT NOT NULL DEFAULT 'fr',
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

  CONSTRAINT chk_france_source_action_type CHECK (
    action_type IN ('translate_field', 'discover_domains', 'build_suggestion')
  ),
  CONSTRAINT chk_france_source_action_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_france_source_action_attempt CHECK (attempt_count >= 0 AND max_attempts > 0),
  CONSTRAINT chk_france_source_action_result_object CHECK (jsonb_typeof(result) = 'object'),
  CONSTRAINT chk_france_source_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)
);

CREATE INDEX idx_france_source_action_queue
  ON france_source.action_tasks (action_type, status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX idx_france_source_action_company
  ON france_source.action_tasks (company_id, action_type, status);

CREATE TABLE france_source.translation_terms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL DEFAULT 'france',
  source_lang TEXT NOT NULL DEFAULT 'fr',
  target_lang TEXT NOT NULL DEFAULT 'en',
  source_text_normalized TEXT NOT NULL,
  source_text TEXT NOT NULL,
  term_key TEXT NOT NULL,
  translated_text TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  provider TEXT,
  model TEXT,
  prompt_version TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_requested_at TIMESTAMPTZ,
  translated_at TIMESTAMPTZ,
  error TEXT,
  error_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_france_source_translation_terms_status CHECK (
    status IN ('pending', 'queued', 'running', 'succeeded', 'failed_retryable', 'failed_terminal')
  ),
  CONSTRAINT chk_france_source_translation_terms_attempt CHECK (attempt_count >= 0),
  CONSTRAINT chk_france_source_translation_terms_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (source, source_lang, target_lang, prompt_version, term_key)
);

CREATE INDEX idx_france_source_translation_terms_queue
  ON france_source.translation_terms (status, updated_at)
  WHERE status IN ('pending', 'queued', 'failed_retryable');

CREATE OR REPLACE VIEW france_source.v_missing_translations AS
SELECT
  company.id AS company_id,
  'france_source.companies'::text AS source_table,
  company.id AS source_row_id,
  'registration_status_label'::text AS source_column,
  'registration_status_label_en'::text AS target_column,
  company.registration_status_label AS source_text,
  encode(digest(company.registration_status_label, 'sha256'), 'hex') AS source_text_hash,
  20::integer AS priority
FROM france_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.registration_status_label), '') IS NOT NULL
  AND nullif(btrim(company.registration_status_label_en), '') IS NULL
UNION ALL
SELECT company.id, 'france_source.companies', company.id, 'legal_form_label', 'legal_form_label_en', company.legal_form_label, encode(digest(company.legal_form_label, 'sha256'), 'hex'), 20
FROM france_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.legal_form_label), '') IS NOT NULL
  AND nullif(btrim(company.legal_form_label_en), '') IS NULL
UNION ALL
SELECT company.id, 'france_source.companies', company.id, 'company_category_label', 'company_category_label_en', company.company_category_label, encode(digest(company.company_category_label, 'sha256'), 'hex'), 30
FROM france_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.company_category_label), '') IS NOT NULL
  AND nullif(btrim(company.company_category_label_en), '') IS NULL
UNION ALL
SELECT industry.company_id, 'france_source.industries', industry.id, 'source_label', 'source_label_en', industry.source_label, encode(digest(industry.source_label, 'sha256'), 'hex'), 20
FROM france_source.industries industry
WHERE nullif(btrim(industry.source_label), '') IS NOT NULL
  AND nullif(btrim(industry.source_label_en), '') IS NULL
UNION ALL
SELECT address.company_id, 'france_source.addresses', address.id, 'foreign_country', 'foreign_country_en', address.foreign_country, encode(digest(address.foreign_country, 'sha256'), 'hex'), 50
FROM france_source.addresses address
WHERE nullif(btrim(address.foreign_country), '') IS NOT NULL
  AND nullif(btrim(address.foreign_country_en), '') IS NULL;

CREATE OR REPLACE VIEW france_source.v_company_explorer AS
WITH headquarters AS (
  SELECT DISTINCT ON (company_id)
    id AS establishment_id,
    company_id,
    siret,
    establishment_name,
    lifecycle_status AS establishment_lifecycle_status
  FROM france_source.establishments
  WHERE row_status = 'active'
  ORDER BY company_id, is_headquarters DESC, updated_at DESC
),
primary_address AS (
  SELECT DISTINCT ON (company_id)
    company_id,
    establishment_id,
    postal_code,
    city,
    commune_code,
    formatted_address,
    latitude,
    longitude,
    geocode_status
  FROM france_source.addresses
  ORDER BY company_id, (address_type = 'headquarters') DESC, address_rank ASC
),
primary_industry AS (
  SELECT DISTINCT ON (company_id)
    company_id,
    source_code AS primary_industry_code,
    coalesce(source_label_en, source_label) AS primary_industry_label,
    mapped_nace_code AS primary_nace_code,
    coalesce(nace_title_en, nace_title) AS primary_nace_title
  FROM france_source.industries
  ORDER BY company_id, is_primary DESC, position ASC
),
establishment_counts AS (
  SELECT company_id, count(*)::bigint AS establishment_count
  FROM france_source.establishments
  WHERE row_status = 'active'
  GROUP BY company_id
),
website_counts AS (
  SELECT company_id, count(*)::bigint AS website_count
  FROM france_source.websites
  WHERE status = 'active'
  GROUP BY company_id
),
domain_counts AS (
  SELECT company_id, count(*)::bigint AS domain_count
  FROM france_source.domains
  WHERE status = 'active'
  GROUP BY company_id
),
contact_counts AS (
  SELECT company_id, count(*)::bigint AS contact_count
  FROM france_source.contacts
  WHERE status = 'active'
  GROUP BY company_id
),
translation_counts AS (
  SELECT company_id, count(*)::bigint AS translation_missing_count
  FROM france_source.v_missing_translations
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
  FROM france_source.action_tasks
  GROUP BY company_id
)
SELECT
  company.id AS company_id,
  company.siren,
  company.organization_name,
  company.description_en,
  company.lifecycle_status,
  company.registration_status,
  company.legal_form_code,
  coalesce(company.legal_form_label_en, company.legal_form_label) AS legal_form_label,
  company.company_category,
  industry.primary_industry_code,
  industry.primary_industry_label,
  industry.primary_nace_code,
  industry.primary_nace_title,
  headquarters.siret AS headquarters_siret,
  address.city,
  address.commune_code,
  address.postal_code,
  address.formatted_address,
  address.latitude,
  address.longitude,
  address.geocode_status,
  company.employee_band,
  coalesce(establishment_counts.establishment_count, 0) AS establishment_count,
  coalesce(website_counts.website_count, 0) AS website_count,
  coalesce(domain_counts.domain_count, 0) AS domain_count,
  coalesce(contact_counts.contact_count, 0) AS contact_count,
  coalesce(translation_counts.translation_missing_count, 0) AS translation_missing_count,
  coalesce(action_counts.translation_pending_count, 0) AS translation_pending_count,
  coalesce(action_counts.translation_running_count, 0) AS translation_running_count,
  coalesce(action_counts.translation_succeeded_count, 0) AS translation_succeeded_count,
  coalesce(action_counts.domain_pending_count, 0) AS domain_pending_count,
  coalesce(action_counts.domain_running_count, 0) AS domain_running_count,
  coalesce(action_counts.domain_succeeded_count, 0) AS domain_succeeded_count,
  company.updated_at
FROM france_source.companies company
LEFT JOIN headquarters ON headquarters.company_id = company.id
LEFT JOIN primary_address address ON address.company_id = company.id
LEFT JOIN primary_industry industry ON industry.company_id = company.id
LEFT JOIN establishment_counts ON establishment_counts.company_id = company.id
LEFT JOIN website_counts ON website_counts.company_id = company.id
LEFT JOIN domain_counts ON domain_counts.company_id = company.id
LEFT JOIN contact_counts ON contact_counts.company_id = company.id
LEFT JOIN translation_counts ON translation_counts.company_id = company.id
LEFT JOIN action_counts ON action_counts.company_id = company.id
WHERE company.row_status = 'active';

CREATE OR REPLACE VIEW france_source.v_company_detail AS
WITH establishment_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(establishments) - 'company_id' ORDER BY is_headquarters DESC, siret) AS establishments
  FROM france_source.establishments
  WHERE row_status = 'active'
  GROUP BY company_id
),
address_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(addresses) - 'company_id' ORDER BY address_type, address_rank) AS addresses
  FROM france_source.addresses
  GROUP BY company_id
),
industry_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(industries) - 'company_id' ORDER BY is_primary DESC, source_field, position) AS industries
  FROM france_source.industries
  GROUP BY company_id
),
website_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(websites) - 'company_id' ORDER BY is_primary DESC, confidence DESC NULLS LAST, created_at DESC) AS websites
  FROM france_source.websites
  GROUP BY company_id
),
domain_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(domains) - 'company_id' ORDER BY is_primary DESC, confidence DESC, created_at DESC) AS domains
  FROM france_source.domains
  GROUP BY company_id
),
contact_rows AS (
  SELECT company_id, jsonb_agg(to_jsonb(contacts) - 'company_id' ORDER BY is_primary DESC, contact_type, created_at DESC) AS contacts
  FROM france_source.contacts
  GROUP BY company_id
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
  FROM france_source.action_tasks
  WHERE action_type = 'translate_field'
  GROUP BY company_id
)
SELECT
  company.*,
  coalesce(establishment_rows.establishments, '[]'::jsonb) AS establishments,
  coalesce(address_rows.addresses, '[]'::jsonb) AS addresses,
  coalesce(industry_rows.industries, '[]'::jsonb) AS industries,
  coalesce(website_rows.websites, '[]'::jsonb) AS websites,
  coalesce(domain_rows.domains, '[]'::jsonb) AS domains,
  coalesce(contact_rows.contacts, '[]'::jsonb) AS contacts,
  coalesce(translation_rows.translation_status, '{}'::jsonb) AS translation_status
FROM france_source.companies company
LEFT JOIN establishment_rows ON establishment_rows.company_id = company.id
LEFT JOIN address_rows ON address_rows.company_id = company.id
LEFT JOIN industry_rows ON industry_rows.company_id = company.id
LEFT JOIN website_rows ON website_rows.company_id = company.id
LEFT JOIN domain_rows ON domain_rows.company_id = company.id
LEFT JOIN contact_rows ON contact_rows.company_id = company.id
LEFT JOIN translation_rows ON translation_rows.company_id = company.id
WHERE company.row_status = 'active';

GRANT USAGE ON SCHEMA france_source TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA france_source TO corpscout_anon;
