# BRREG Source Profile Tables Suggestion

## Summary

Create a proper BRREG source-specific profile schema for browsing Norwegian companies in a Yra-like way. The schema should be source-owned, denormalized for exploration, and separate from the central Corpscout company identity tables.

The proposed boundary is:

- `brreg_workflow.*`: raw records, workflow runs, task attempts, translation/domain/financial artifacts.
- `brreg_source.*`: final BRREG-specific company profile tables used by UI/search.
- `public.*`: cross-source identity only, such as canonical companies, domains, websites, names, NACE codes, and links to source profiles.

This suggestion replaces the old public `brreg_source_*` table direction with a real `brreg_source` schema. The old public tables can be migrated or dropped when this design is implemented.

## Principles

- Source-specific facts stay in source-specific tables.
- Central Corpscout tables only store cross-source identity and common facts.
- Every source table keeps provenance back to `brreg_workflow.raw_records`.
- Repeated entities get their own tables: websites, domains, contacts, industries, financial years, roles, owners.
- English display text is stored directly in `_en` columns next to the original source values.
- Company names, proper nouns, city names, domains, and URLs are not translated by default.
- Domain and website are different concepts. A Facebook page is a website/contact channel, not the company's official domain.
- Source profile build is one Temporal workflow; translation can run continuously as a field-level queue.

## Proposed Schema

```sql
CREATE SCHEMA IF NOT EXISTS brreg_source;
```

All tables below should include:

- `raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id)`
- optional references to source artifacts such as `translation_result_id`, `domain_result_id`, `financial_result_id`, `enhanced_record_id`
- `evidence JSONB NOT NULL DEFAULT '{}'::jsonb`
- `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`

## brreg_source.companies

One active row per current BRREG organization number. This is the anchor table for the source profile.

```sql
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

CREATE INDEX idx_brreg_source_companies_name
  ON brreg_source.companies(organization_name_normalized);

CREATE INDEX idx_brreg_source_companies_status
  ON brreg_source.companies(row_status, lifecycle_status, updated_at DESC);
```

## brreg_source.addresses

Business and postal addresses. Keep original place names; translate only labels where needed.

```sql
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
  UNIQUE (company_id, address_type)
);

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
```

## brreg_source.industries

BRREG industry fields plus mapping to global NACE taxonomy.

```sql
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
  UNIQUE (company_id, classification_type, position)
);

CREATE INDEX idx_brreg_source_industries_source_code
  ON brreg_source.industries(classification_type, source_code);

CREATE INDEX idx_brreg_source_industries_nace
  ON brreg_source.industries(nace_code_id)
  WHERE nace_code_id IS NOT NULL;
```

## brreg_source.websites

Many URLs per company. This table stores actual pages, including social pages and registry-provided websites.

```sql
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
  CONSTRAINT chk_brreg_source_websites_confidence CHECK (confidence IS NULL OR confidence BETWEEN 1 AND 100)
);

CREATE UNIQUE INDEX uq_brreg_source_websites_active_url
  ON brreg_source.websites(company_id, normalized_url)
  WHERE status = 'active';

CREATE INDEX idx_brreg_source_websites_host
  ON brreg_source.websites(host)
  WHERE host IS NOT NULL;
```

## brreg_source.domains

Many domains per company. Do not insert `facebook.com` as a company domain just because the company has a Facebook page.

```sql
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
  CONSTRAINT chk_brreg_source_domains_confidence CHECK (confidence BETWEEN 1 AND 100)
);

CREATE UNIQUE INDEX uq_brreg_source_domains_active
  ON brreg_source.domains(company_id, normalized_domain)
  WHERE status = 'active';

CREATE INDEX idx_brreg_source_domains_domain
  ON brreg_source.domains(normalized_domain);
```

## brreg_source.contacts

Phone, email, contact form, and other non-website channels. Email domains can create candidate rows in `brreg_source.domains`, but not automatically official domains.

```sql
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
  CONSTRAINT chk_brreg_source_contacts_confidence CHECK (confidence IS NULL OR confidence BETWEEN 1 AND 100)
);

CREATE UNIQUE INDEX uq_brreg_source_contacts_active
  ON brreg_source.contacts(company_id, contact_type, normalized_value)
  WHERE status = 'active' AND normalized_value IS NOT NULL;
```

## brreg_source.capital

Share capital from BRREG. Store original currency and USD conversion.

```sql
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
  UNIQUE (company_id)
);
```

## brreg_source.financial_statements

One row per company, fiscal year, statement type, and consolidation scope. This is a wide table for direct exploration and sorting.

```sql
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
```

## brreg_source.people_and_organizations

Role holders and owners can be people or organizations. Keep this source-local to avoid polluting central identity before resolution.

```sql
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
  CONSTRAINT chk_brreg_source_holders_birth_year CHECK (birth_year IS NULL OR birth_year BETWEEN 1800 AND 2200)
);

CREATE INDEX idx_brreg_source_holders_name
  ON brreg_source.people_and_organizations(display_name_normalized);

CREATE INDEX idx_brreg_source_holders_org
  ON brreg_source.people_and_organizations(organization_number)
  WHERE organization_number IS NOT NULL;
```

## brreg_source.roles

Board and management roles if the source is added later.

```sql
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
  UNIQUE (company_id, holder_id, role_code, started_at)
);
```

## brreg_source.shareholdings

Ownership rows if shareholder data is added later. This enables Yra-like owner/shareholder views.

```sql
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
  UNIQUE (company_id, holder_id, fiscal_year, share_class)
);
```

## brreg_source.field_translation_tasks

This is the continuous translation queue for `_en` columns. It stores work state, not duplicate business facts. The source text remains in the source profile tables; the translated value is applied back to the relevant `_en` column.

```sql
CREATE TABLE brreg_source.field_translation_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,

  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  source_column TEXT NOT NULL,
  target_column TEXT NOT NULL,
  source_text_hash TEXT NOT NULL,
  source_text TEXT NOT NULL,
  source_lang TEXT NOT NULL DEFAULT 'no',
  target_lang TEXT NOT NULL DEFAULT 'en',

  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  lease_until TIMESTAMPTZ,
  last_started_at TIMESTAMPTZ,
  last_finished_at TIMESTAMPTZ,
  translated_text TEXT,
  model TEXT,
  prompt_version TEXT,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,

  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_translation_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_brreg_source_translation_attempt CHECK (attempt_count >= 0 AND max_attempts > 0),
  CONSTRAINT chk_brreg_source_translation_source CHECK (btrim(source_text) <> ''),
  UNIQUE (source_table, source_row_id, source_column, target_column, source_text_hash)
);

CREATE INDEX idx_brreg_source_translation_queue
  ON brreg_source.field_translation_tasks(status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX idx_brreg_source_translation_company
  ON brreg_source.field_translation_tasks(company_id, status);
```

## Read Views

### brreg_source.v_company_explorer

One row per company for the main source explorer table. It should include counts and best values rather than embedding arrays.

Columns:

- `company_id`
- `organization_number`
- `organization_name`
- `description_en`
- `lifecycle_status`
- `organization_form_label_en`
- `primary_industry_code`
- `primary_industry_label_en`
- `primary_nace_code`
- `primary_nace_title`
- `city`
- `municipality`
- `employee_count`
- `employee_band`
- `website_count`
- `domain_count`
- `contact_count`
- `latest_financial_year`
- `latest_revenue_usd_cents`
- `latest_total_assets_usd_cents`
- `latest_net_income_usd_cents`
- `translation_missing_count`
- `updated_at`

### brreg_source.v_company_detail

One row per company with JSON arrays for detail UI:

- `addresses`
- `industries`
- `websites`
- `domains`
- `contacts`
- `financial_years`
- `roles`
- `shareholders`
- `translation_status`

### brreg_source.v_missing_translations

A read view over source tables that shows missing `_en` fields. This should feed `field_translation_tasks`.

Example fields:

- `company_id`
- `source_table`
- `source_row_id`
- `source_column`
- `target_column`
- `source_text`
- `source_text_hash`
- `priority`

## Temporal Workflows

### Build BRREG Source Profile

Workflow name: `BuildBrregSourceProfiles`

Actions:

1. `ClaimBrregSourceProfileBatch`
   - Select current `brreg_workflow.raw_records` with all required artifacts available.
   - Do not require domain discovery in the first version.

2. `BuildBrregSourceProfileObject`
   - Parse BRREG raw payload.
   - Merge translation results where available.
   - Merge financial conversion results where available.
   - Merge current domain/website/contact artifacts where available.
   - Produce a typed profile object.

3. `StoreBrregSourceProfile`
   - Upsert `brreg_source.companies`.
   - Replace child rows for addresses, industries, websites, domains, contacts, capital, financials in one transaction.
   - Supersede old active rows when payload hash/profile version changes.

4. `QueueBrregSourceTranslations`
   - Read `brreg_source.v_missing_translations`.
   - Insert missing `field_translation_tasks`.

### Translate BRREG Source Profile Fields

Workflow name: `TranslateBrregSourceFields`

Actions:

1. `ClaimBrregSourceTranslationBatch`
   - Claim pending/retryable field translation tasks.

2. `TranslateBrregSourceFieldBatch`
   - Send source texts to translation service.

3. `ApplyBrregSourceTranslations`
   - Update the target `_en` columns.
   - Mark tasks succeeded/failed in one transaction.

### Geocode BRREG Source Addresses

Workflow name: `GeocodeBrregSourceAddresses`

This can run after source profiles are built. It is not required for the first source-profile build because the UI can still open Google Maps or OpenStreetMap search links using `formatted_address`.

Actions:

1. `ClaimBrregAddressGeocodeBatch`
   - Claim `brreg_source.addresses` rows with `geocode_status IN ('not_attempted', 'queued', 'failed', 'ambiguous')`.
   - Prefer rows with complete street, postal code, and municipality.

2. `GeocodeBrregAddressBatch`
   - First provider for Norway should be Kartverket / Geonorge Adresse API.
   - Store coordinates only when the result is sufficiently specific.

3. `ApplyBrregAddressGeocodes`
   - Update `latitude`, `longitude`, `coordinate_system`, `geocode_provider`, `geocode_provider_place_id`, `geocode_confidence`, `geocode_precision`, `geocoded_at`, and `geocode_payload`.
   - Mark ambiguous or not-found rows explicitly so they are explorable and retryable.

## First Implementation Slice

The first migration should create only the tables needed for the current product goal:

1. `brreg_source.companies`
2. `brreg_source.addresses`
3. `brreg_source.industries`
4. `brreg_source.websites`
5. `brreg_source.domains`
6. `brreg_source.contacts`
7. `brreg_source.capital`
8. `brreg_source.financial_statements`
9. `brreg_source.field_translation_tasks`
10. `brreg_source.v_company_explorer`
11. `brreg_source.v_company_detail`
12. `brreg_source.v_missing_translations`

Roles and shareholdings should be created when we add those official data sources.

## Open Implementation Decisions

- Whether to migrate old public `brreg_source_*` tables or drop/recreate them after the new schema is ready.
- Whether `organization_name_en` should be populated. Legal company names usually should not be translated, but a separate English display name can be useful for transliteration or generated summaries.
- Whether geocoding should be in the first profile workflow or later.
- Whether `field_translation_tasks` should stay in `brreg_source` or move to `brreg_workflow`. This suggestion keeps it in `brreg_source` because it is tied to source-profile `_en` columns.
