package ariregisterdb

var sourceProfileCompanyStageColumns = []string{
	"raw_record_id",
	"registry_code",
	"source_native_id",
	"country_iso2",
	"legal_name",
	"legal_name_normalized",
	"legal_name_en",
	"registration_status",
	"registration_status_label",
	"registration_status_label_en",
	"lifecycle_status",
	"legal_form_code",
	"legal_form_number",
	"legal_form_label",
	"legal_form_label_en",
	"legal_form_subtype",
	"legal_form_subtype_label",
	"legal_form_subtype_label_en",
	"region_code",
	"region_label",
	"region_label_en",
	"region_label_long",
	"region_label_long_en",
	"active_label",
	"active_label_en",
	"first_registered_on",
	"deleted_on",
	"evks_registered_at",
	"has_missing_beneficial_owner_discrepancy_notice",
	"founded_without_contribution",
	"waived_form_requirements",
	"is_accounting_required",
	"reports_beneficial_owners",
	"is_active",
	"last_annual_report_year",
	"employee_count",
	"employee_count_source",
	"employee_band",
	"source_updated_at",
	"payload_hash",
	"profile_version",
	"row_status",
	"normalized_payload",
	"raw_company_payload",
	"evidence",
	"metadata",
}

var sourceProfileCompanyNameStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"card_region",
	"card_number",
	"card_type",
	"entry_number",
	"name",
	"name_en",
	"started_on",
	"ended_on",
	"raw_name_payload",
	"evidence",
	"metadata",
}

var sourceProfileCompanyStatusStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"card_region",
	"card_number",
	"card_type",
	"entry_number",
	"status_code",
	"status_label",
	"status_label_en",
	"started_on",
	"raw_status_payload",
	"evidence",
	"metadata",
}

var sourceProfileLegalFormStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"card_region",
	"card_number",
	"card_type",
	"entry_number",
	"legal_form_code",
	"legal_form_number",
	"legal_form_label",
	"legal_form_label_en",
	"legal_form_subtype",
	"legal_form_subtype_label",
	"legal_form_subtype_label_en",
	"started_on",
	"ended_on",
	"raw_legal_form_payload",
	"evidence",
	"metadata",
}

var sourceProfileAddressStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"address_type",
	"country_code",
	"country_label",
	"country_label_en",
	"ehak_code",
	"ehak_name",
	"ehak_name_en",
	"street_text",
	"street_text_en",
	"postal_code",
	"ads_oid",
	"adr_id",
	"normalized_full_address",
	"normalized_full_address_en",
	"normalized_full_address_detail",
	"code_address",
	"adob_id",
	"ads_type",
	"started_on",
	"ended_on",
	"raw_address_payload",
	"evidence",
	"metadata",
}

var sourceProfileContactStageColumns = []string{
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"contact_type",
	"contact_type_label",
	"contact_type_label_en",
	"value",
	"normalized_value",
	"source",
	"status",
	"is_primary",
	"ended_on",
	"evidence",
	"raw_contact_payload",
	"metadata",
}

var sourceProfileWebsiteStageColumns = []string{
	"registry_code",
	"raw_record_id",
	"url",
	"normalized_url",
	"host",
	"path",
	"website_type",
	"source",
	"status",
	"confidence",
	"is_primary",
	"title",
	"title_en",
	"description",
	"description_en",
	"evidence",
	"metadata",
}

var sourceProfileDomainStageColumns = []string{
	"registry_code",
	"raw_record_id",
	"website_normalized",
	"domain",
	"normalized_domain",
	"registrable_domain",
	"domain_type",
	"source",
	"status",
	"confidence",
	"is_primary",
	"best_signal",
	"evidence",
	"metadata",
}

var sourceProfileIndustryStageColumns = []string{
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"classification_type",
	"source_field",
	"position",
	"emtak_code",
	"emtak_label",
	"emtak_label_en",
	"emtak_version",
	"emtak_version_label",
	"emtak_version_label_en",
	"nace_code",
	"nace_revision",
	"nace_title",
	"nace_title_en",
	"mapping_method",
	"mapping_confidence",
	"is_primary",
	"started_on",
	"ended_on",
	"raw_industry_payload",
	"evidence",
	"metadata",
}

var sourceProfileCapitalStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"capital_amount",
	"capital_currency",
	"capital_currency_label",
	"capital_currency_label_en",
	"introduced_on",
	"ended_on",
	"raw_capital_payload",
	"evidence",
	"metadata",
}

var sourceProfileFinancialYearPeriodStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"period_start_month_day",
	"period_end_month_day",
	"started_on",
	"ended_on",
	"raw_period_payload",
	"evidence",
	"metadata",
}

var sourceProfileAnnualReportStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"fiscal_year",
	"period_start",
	"period_end",
	"employee_count",
	"report_address",
	"report_address_en",
	"activity_emtak_code",
	"activity_label",
	"activity_label_en",
	"activity_version",
	"activity_version_label",
	"activity_version_label_en",
	"activity_nace_code",
	"raw_report_payload",
	"evidence",
	"metadata",
}

var sourceProfileArticleStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"confirmed_on",
	"changed_on",
	"explanation",
	"explanation_en",
	"contains_special_rights",
	"started_on",
	"ended_on",
	"raw_articles_payload",
	"evidence",
	"metadata",
}

var sourceProfileRegistryNoteStageColumns = []string{
	"stage_key",
	"registry_code",
	"raw_record_id",
	"source_entry_id",
	"card_region",
	"card_number",
	"card_type",
	"entry_number",
	"column_number",
	"note_type",
	"note_type_label",
	"note_type_label_en",
	"note_text",
	"note_text_en",
	"started_on",
	"ended_on",
	"raw_note_payload",
	"evidence",
	"metadata",
}

const createSourceProfileCopyStageTablesSQL = `
CREATE TEMP TABLE ariregister_source_company_stage (
  raw_record_id text NOT NULL,
  registry_code text NOT NULL,
  source_native_id text NOT NULL,
  country_iso2 text NOT NULL,
  legal_name text NOT NULL,
  legal_name_normalized text NOT NULL,
  legal_name_en text,
  registration_status text,
  registration_status_label text,
  registration_status_label_en text,
  lifecycle_status text NOT NULL,
  legal_form_code text,
  legal_form_number integer,
  legal_form_label text,
  legal_form_label_en text,
  legal_form_subtype text,
  legal_form_subtype_label text,
  legal_form_subtype_label_en text,
  region_code integer,
  region_label text,
  region_label_en text,
  region_label_long text,
  region_label_long_en text,
  active_label text,
  active_label_en text,
  first_registered_on text,
  deleted_on text,
  evks_registered_at text,
  has_missing_beneficial_owner_discrepancy_notice boolean,
  founded_without_contribution boolean,
  waived_form_requirements boolean,
  is_accounting_required boolean,
  reports_beneficial_owners boolean,
  is_active boolean,
  last_annual_report_year integer,
  employee_count integer,
  employee_count_source text,
  employee_band text,
  source_updated_at timestamptz,
  payload_hash text NOT NULL,
  profile_version text NOT NULL,
  row_status text NOT NULL,
  normalized_payload text NOT NULL,
  raw_company_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_company_id_stage (
  registry_code text PRIMARY KEY,
  company_id uuid NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_company_name_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  card_region integer,
  card_number integer,
  card_type text,
  entry_number integer,
  name text NOT NULL,
  name_en text,
  started_on text,
  ended_on text,
  raw_name_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_company_status_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  card_region integer,
  card_number integer,
  card_type text,
  entry_number integer,
  status_code text NOT NULL,
  status_label text,
  status_label_en text,
  started_on text,
  raw_status_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_legal_form_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  card_region integer,
  card_number integer,
  card_type text,
  entry_number integer,
  legal_form_code text,
  legal_form_number integer,
  legal_form_label text,
  legal_form_label_en text,
  legal_form_subtype text,
  legal_form_subtype_label text,
  legal_form_subtype_label_en text,
  started_on text,
  ended_on text,
  raw_legal_form_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_address_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  address_type text NOT NULL,
  country_code text,
  country_label text,
  country_label_en text,
  ehak_code text,
  ehak_name text,
  ehak_name_en text,
  street_text text,
  street_text_en text,
  postal_code text,
  ads_oid text,
  adr_id bigint,
  normalized_full_address text,
  normalized_full_address_en text,
  normalized_full_address_detail text,
  code_address text,
  adob_id text,
  ads_type text,
  started_on text,
  ended_on text,
  raw_address_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_contact_stage (
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  contact_type text NOT NULL,
  contact_type_label text,
  contact_type_label_en text,
  value text NOT NULL,
  normalized_value text NOT NULL,
  source text NOT NULL,
  status text NOT NULL,
  is_primary boolean NOT NULL,
  ended_on text,
  evidence text NOT NULL,
  raw_contact_payload text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_website_stage (
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  url text NOT NULL,
  normalized_url text NOT NULL,
  host text,
  path text,
  website_type text NOT NULL,
  source text NOT NULL,
  status text NOT NULL,
  confidence smallint NOT NULL,
  is_primary boolean NOT NULL,
  title text,
  title_en text,
  description text,
  description_en text,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_website_id_stage (
  registry_code text NOT NULL,
  normalized_url text NOT NULL,
  website_id uuid NOT NULL,
  PRIMARY KEY (registry_code, normalized_url)
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_domain_stage (
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  website_normalized text NOT NULL,
  domain text NOT NULL,
  normalized_domain text NOT NULL,
  registrable_domain text NOT NULL,
  domain_type text NOT NULL,
  source text NOT NULL,
  status text NOT NULL,
  confidence smallint NOT NULL,
  is_primary boolean NOT NULL,
  best_signal text,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_industry_stage (
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  classification_type text NOT NULL,
  source_field text NOT NULL,
  position smallint NOT NULL,
  emtak_code text NOT NULL,
  emtak_label text,
  emtak_label_en text,
  emtak_version integer,
  emtak_version_label text,
  emtak_version_label_en text,
  nace_code text,
  nace_revision text,
  nace_title text,
  nace_title_en text,
  mapping_method text,
  mapping_confidence real,
  is_primary boolean NOT NULL,
  started_on text,
  ended_on text,
  raw_industry_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_capital_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  capital_amount text,
  capital_currency text,
  capital_currency_label text,
  capital_currency_label_en text,
  introduced_on text,
  ended_on text,
  raw_capital_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_financial_year_period_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  period_start_month_day text,
  period_end_month_day text,
  started_on text,
  ended_on text,
  raw_period_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_annual_report_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  fiscal_year integer,
  period_start text,
  period_end text,
  employee_count integer,
  report_address text,
  report_address_en text,
  activity_emtak_code text,
  activity_label text,
  activity_label_en text,
  activity_version text,
  activity_version_label text,
  activity_version_label_en text,
  activity_nace_code text,
  raw_report_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_article_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  confirmed_on text,
  changed_on text,
  explanation text,
  explanation_en text,
  contains_special_rights boolean,
  started_on text,
  ended_on text,
  raw_articles_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE ariregister_source_registry_note_stage (
  stage_key text NOT NULL,
  registry_code text NOT NULL,
  raw_record_id text NOT NULL,
  source_entry_id bigint,
  card_region integer,
  card_number integer,
  card_type text,
  entry_number integer,
  column_number integer,
  note_type text,
  note_type_label text,
  note_type_label_en text,
  note_text text,
  note_text_en text,
  started_on text,
  ended_on text,
  raw_note_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;
`

const supersedeSourceProfileCompaniesSQL = `
UPDATE ariregister_source.companies company
SET
  row_status = 'superseded',
  superseded_at = now(),
  updated_at = now()
FROM ariregister_source_company_stage stage
WHERE company.registry_code = stage.registry_code
  AND company.row_status = 'active'
  AND company.payload_hash IS DISTINCT FROM stage.payload_hash;
`

const mergeSourceProfileCompaniesSQL = `
WITH upserted AS (
  INSERT INTO ariregister_source.companies (
    raw_record_id,
    registry_code,
    source_native_id,
    country_iso2,
    legal_name,
    legal_name_normalized,
    legal_name_en,
    registration_status,
    registration_status_label,
    registration_status_label_en,
    lifecycle_status,
    legal_form_code,
    legal_form_number,
    legal_form_label,
    legal_form_label_en,
    legal_form_subtype,
    legal_form_subtype_label,
    legal_form_subtype_label_en,
    region_code,
    region_label,
    region_label_en,
    region_label_long,
    region_label_long_en,
    active_label,
    active_label_en,
    first_registered_on,
    deleted_on,
    evks_registered_at,
    has_missing_beneficial_owner_discrepancy_notice,
    founded_without_contribution,
    waived_form_requirements,
    is_accounting_required,
    reports_beneficial_owners,
    is_active,
    last_annual_report_year,
    employee_count,
    employee_count_source,
    employee_band,
    source_updated_at,
    payload_hash,
    profile_version,
    row_status,
    normalized_payload,
    raw_company_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    raw_record_id::uuid,
    registry_code,
    source_native_id,
    country_iso2,
    legal_name,
    legal_name_normalized,
    NULLIF(legal_name_en, ''),
    NULLIF(registration_status, ''),
    NULLIF(registration_status_label, ''),
    NULLIF(registration_status_label_en, ''),
    lifecycle_status,
    NULLIF(legal_form_code, ''),
    legal_form_number,
    NULLIF(legal_form_label, ''),
    NULLIF(legal_form_label_en, ''),
    NULLIF(legal_form_subtype, ''),
    NULLIF(legal_form_subtype_label, ''),
    NULLIF(legal_form_subtype_label_en, ''),
    region_code,
    NULLIF(region_label, ''),
    NULLIF(region_label_en, ''),
    NULLIF(region_label_long, ''),
    NULLIF(region_label_long_en, ''),
    NULLIF(active_label, ''),
    NULLIF(active_label_en, ''),
    NULLIF(first_registered_on, '')::date,
    NULLIF(deleted_on, '')::date,
    NULLIF(evks_registered_at, '')::date,
    has_missing_beneficial_owner_discrepancy_notice,
    founded_without_contribution,
    waived_form_requirements,
    is_accounting_required,
    reports_beneficial_owners,
    is_active,
    last_annual_report_year,
    employee_count,
    NULLIF(employee_count_source, ''),
    NULLIF(employee_band, ''),
    source_updated_at,
    payload_hash,
    profile_version,
    row_status,
    normalized_payload::jsonb,
    raw_company_payload::jsonb,
    evidence::jsonb,
    metadata::jsonb,
    now()
  FROM ariregister_source_company_stage
  ON CONFLICT (registry_code) WHERE row_status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    source_native_id = EXCLUDED.source_native_id,
    country_iso2 = EXCLUDED.country_iso2,
    legal_name = EXCLUDED.legal_name,
    legal_name_normalized = EXCLUDED.legal_name_normalized,
    legal_name_en = COALESCE(EXCLUDED.legal_name_en, ariregister_source.companies.legal_name_en),
    registration_status = EXCLUDED.registration_status,
    registration_status_label = EXCLUDED.registration_status_label,
    registration_status_label_en = COALESCE(EXCLUDED.registration_status_label_en, ariregister_source.companies.registration_status_label_en),
    lifecycle_status = EXCLUDED.lifecycle_status,
    legal_form_code = EXCLUDED.legal_form_code,
    legal_form_number = EXCLUDED.legal_form_number,
    legal_form_label = EXCLUDED.legal_form_label,
    legal_form_label_en = COALESCE(EXCLUDED.legal_form_label_en, ariregister_source.companies.legal_form_label_en),
    legal_form_subtype = EXCLUDED.legal_form_subtype,
    legal_form_subtype_label = EXCLUDED.legal_form_subtype_label,
    legal_form_subtype_label_en = COALESCE(EXCLUDED.legal_form_subtype_label_en, ariregister_source.companies.legal_form_subtype_label_en),
    region_code = EXCLUDED.region_code,
    region_label = EXCLUDED.region_label,
    region_label_en = COALESCE(EXCLUDED.region_label_en, ariregister_source.companies.region_label_en),
    region_label_long = EXCLUDED.region_label_long,
    region_label_long_en = COALESCE(EXCLUDED.region_label_long_en, ariregister_source.companies.region_label_long_en),
    active_label = EXCLUDED.active_label,
    active_label_en = COALESCE(EXCLUDED.active_label_en, ariregister_source.companies.active_label_en),
    first_registered_on = EXCLUDED.first_registered_on,
    deleted_on = EXCLUDED.deleted_on,
    evks_registered_at = EXCLUDED.evks_registered_at,
    has_missing_beneficial_owner_discrepancy_notice = EXCLUDED.has_missing_beneficial_owner_discrepancy_notice,
    founded_without_contribution = EXCLUDED.founded_without_contribution,
    waived_form_requirements = EXCLUDED.waived_form_requirements,
    is_accounting_required = EXCLUDED.is_accounting_required,
    reports_beneficial_owners = EXCLUDED.reports_beneficial_owners,
    is_active = EXCLUDED.is_active,
    last_annual_report_year = EXCLUDED.last_annual_report_year,
    employee_count = EXCLUDED.employee_count,
    employee_count_source = EXCLUDED.employee_count_source,
    employee_band = EXCLUDED.employee_band,
    source_updated_at = EXCLUDED.source_updated_at,
    payload_hash = EXCLUDED.payload_hash,
    profile_version = EXCLUDED.profile_version,
    normalized_payload = EXCLUDED.normalized_payload,
    raw_company_payload = EXCLUDED.raw_company_payload,
    evidence = EXCLUDED.evidence,
    metadata = ariregister_source.companies.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING registry_code, id
),
mapped AS (
  INSERT INTO ariregister_source_company_id_stage (registry_code, company_id)
  SELECT registry_code, id
  FROM upserted
  ON CONFLICT (registry_code) DO UPDATE SET company_id = EXCLUDED.company_id
  RETURNING company_id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileCompanyNamesSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_company_name_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.company_names target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    card_region = stage.card_region,
    card_number = stage.card_number,
    card_type = NULLIF(stage.card_type, ''),
    entry_number = stage.entry_number,
    name = stage.name,
    name_en = COALESCE(NULLIF(stage.name_en, ''), target.name_en),
    started_on = NULLIF(stage.started_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_name_payload = stage.raw_name_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.name = stage.name
        AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
        AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
        AND target.raw_name_payload = stage.raw_name_payload::jsonb
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.company_names (
    company_id,
    raw_record_id,
    source_entry_id,
    card_region,
    card_number,
    card_type,
    entry_number,
    name,
    name_en,
    started_on,
    ended_on,
    raw_name_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    stage.card_region,
    stage.card_number,
    NULLIF(stage.card_type, ''),
    stage.entry_number,
    stage.name,
    NULLIF(stage.name_en, ''),
    NULLIF(stage.started_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_name_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.company_names target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.name = stage.name
          AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
          AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
          AND target.raw_name_payload = stage.raw_name_payload::jsonb
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileCompanyStatusesSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_company_status_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.company_statuses target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    status_label = NULLIF(stage.status_label, ''),
    status_label_en = COALESCE(NULLIF(stage.status_label_en, ''), target.status_label_en),
    started_on = NULLIF(stage.started_on, '')::date,
    raw_status_payload = stage.raw_status_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND target.card_region IS NOT DISTINCT FROM stage.card_region
    AND target.card_number IS NOT DISTINCT FROM stage.card_number
    AND COALESCE(target.card_type, '') = COALESCE(NULLIF(stage.card_type, ''), '')
    AND target.entry_number IS NOT DISTINCT FROM stage.entry_number
    AND target.status_code = stage.status_code
    AND target.status_label IS NOT DISTINCT FROM NULLIF(stage.status_label, '')
    AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.company_statuses (
    company_id,
    raw_record_id,
    card_region,
    card_number,
    card_type,
    entry_number,
    status_code,
    status_label,
    status_label_en,
    started_on,
    raw_status_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.card_region,
    stage.card_number,
    NULLIF(stage.card_type, ''),
    stage.entry_number,
    stage.status_code,
    NULLIF(stage.status_label, ''),
    NULLIF(stage.status_label_en, ''),
    NULLIF(stage.started_on, '')::date,
    stage.raw_status_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.company_statuses target
    WHERE target.company_id = stage.company_id
      AND target.card_region IS NOT DISTINCT FROM stage.card_region
      AND target.card_number IS NOT DISTINCT FROM stage.card_number
      AND COALESCE(target.card_type, '') = COALESCE(NULLIF(stage.card_type, ''), '')
      AND target.entry_number IS NOT DISTINCT FROM stage.entry_number
      AND target.status_code = stage.status_code
      AND target.status_label IS NOT DISTINCT FROM NULLIF(stage.status_label, '')
      AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileLegalFormsSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_legal_form_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.legal_forms target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    card_region = stage.card_region,
    card_number = stage.card_number,
    card_type = NULLIF(stage.card_type, ''),
    entry_number = stage.entry_number,
    legal_form_code = NULLIF(stage.legal_form_code, ''),
    legal_form_number = stage.legal_form_number,
    legal_form_label = NULLIF(stage.legal_form_label, ''),
    legal_form_label_en = COALESCE(NULLIF(stage.legal_form_label_en, ''), target.legal_form_label_en),
    legal_form_subtype = NULLIF(stage.legal_form_subtype, ''),
    legal_form_subtype_label = NULLIF(stage.legal_form_subtype_label, ''),
    legal_form_subtype_label_en = COALESCE(NULLIF(stage.legal_form_subtype_label_en, ''), target.legal_form_subtype_label_en),
    started_on = NULLIF(stage.started_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_legal_form_payload = stage.raw_legal_form_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.legal_form_code IS NOT DISTINCT FROM NULLIF(stage.legal_form_code, '')
        AND target.legal_form_number IS NOT DISTINCT FROM stage.legal_form_number
        AND target.legal_form_label IS NOT DISTINCT FROM NULLIF(stage.legal_form_label, '')
        AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
        AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.legal_forms (
    company_id,
    raw_record_id,
    source_entry_id,
    card_region,
    card_number,
    card_type,
    entry_number,
    legal_form_code,
    legal_form_number,
    legal_form_label,
    legal_form_label_en,
    legal_form_subtype,
    legal_form_subtype_label,
    legal_form_subtype_label_en,
    started_on,
    ended_on,
    raw_legal_form_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    stage.card_region,
    stage.card_number,
    NULLIF(stage.card_type, ''),
    stage.entry_number,
    NULLIF(stage.legal_form_code, ''),
    stage.legal_form_number,
    NULLIF(stage.legal_form_label, ''),
    NULLIF(stage.legal_form_label_en, ''),
    NULLIF(stage.legal_form_subtype, ''),
    NULLIF(stage.legal_form_subtype_label, ''),
    NULLIF(stage.legal_form_subtype_label_en, ''),
    NULLIF(stage.started_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_legal_form_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.legal_forms target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.legal_form_code IS NOT DISTINCT FROM NULLIF(stage.legal_form_code, '')
          AND target.legal_form_number IS NOT DISTINCT FROM stage.legal_form_number
          AND target.legal_form_label IS NOT DISTINCT FROM NULLIF(stage.legal_form_label, '')
          AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
          AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileAddressesSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_address_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.addresses target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    address_type = COALESCE(NULLIF(stage.address_type, ''), 'registered'),
    country_code = NULLIF(stage.country_code, ''),
    country_label = NULLIF(stage.country_label, ''),
    country_label_en = COALESCE(NULLIF(stage.country_label_en, ''), target.country_label_en),
    ehak_code = NULLIF(stage.ehak_code, ''),
    ehak_name = NULLIF(stage.ehak_name, ''),
    ehak_name_en = COALESCE(NULLIF(stage.ehak_name_en, ''), target.ehak_name_en),
    street_text = NULLIF(stage.street_text, ''),
    street_text_en = COALESCE(NULLIF(stage.street_text_en, ''), target.street_text_en),
    postal_code = NULLIF(stage.postal_code, ''),
    ads_oid = NULLIF(stage.ads_oid, ''),
    adr_id = stage.adr_id,
    normalized_full_address = NULLIF(stage.normalized_full_address, ''),
    normalized_full_address_en = COALESCE(NULLIF(stage.normalized_full_address_en, ''), target.normalized_full_address_en),
    normalized_full_address_detail = NULLIF(stage.normalized_full_address_detail, ''),
    code_address = NULLIF(stage.code_address, ''),
    adob_id = NULLIF(stage.adob_id, ''),
    ads_type = NULLIF(stage.ads_type, ''),
    started_on = NULLIF(stage.started_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_address_payload = stage.raw_address_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.address_type = COALESCE(NULLIF(stage.address_type, ''), 'registered')
        AND target.normalized_full_address IS NOT DISTINCT FROM NULLIF(stage.normalized_full_address, '')
        AND target.country_code IS NOT DISTINCT FROM NULLIF(stage.country_code, '')
        AND target.ehak_code IS NOT DISTINCT FROM NULLIF(stage.ehak_code, '')
        AND target.street_text IS NOT DISTINCT FROM NULLIF(stage.street_text, '')
        AND target.postal_code IS NOT DISTINCT FROM NULLIF(stage.postal_code, '')
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.addresses (
    company_id,
    raw_record_id,
    source_entry_id,
    address_type,
    country_code,
    country_label,
    country_label_en,
    ehak_code,
    ehak_name,
    ehak_name_en,
    street_text,
    street_text_en,
    postal_code,
    ads_oid,
    adr_id,
    normalized_full_address,
    normalized_full_address_en,
    normalized_full_address_detail,
    code_address,
    adob_id,
    ads_type,
    started_on,
    ended_on,
    raw_address_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    COALESCE(NULLIF(stage.address_type, ''), 'registered'),
    NULLIF(stage.country_code, ''),
    NULLIF(stage.country_label, ''),
    NULLIF(stage.country_label_en, ''),
    NULLIF(stage.ehak_code, ''),
    NULLIF(stage.ehak_name, ''),
    NULLIF(stage.ehak_name_en, ''),
    NULLIF(stage.street_text, ''),
    NULLIF(stage.street_text_en, ''),
    NULLIF(stage.postal_code, ''),
    NULLIF(stage.ads_oid, ''),
    stage.adr_id,
    NULLIF(stage.normalized_full_address, ''),
    NULLIF(stage.normalized_full_address_en, ''),
    NULLIF(stage.normalized_full_address_detail, ''),
    NULLIF(stage.code_address, ''),
    NULLIF(stage.adob_id, ''),
    NULLIF(stage.ads_type, ''),
    NULLIF(stage.started_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_address_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.addresses target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.address_type = COALESCE(NULLIF(stage.address_type, ''), 'registered')
          AND target.normalized_full_address IS NOT DISTINCT FROM NULLIF(stage.normalized_full_address, '')
          AND target.country_code IS NOT DISTINCT FROM NULLIF(stage.country_code, '')
          AND target.ehak_code IS NOT DISTINCT FROM NULLIF(stage.ehak_code, '')
          AND target.street_text IS NOT DISTINCT FROM NULLIF(stage.street_text, '')
          AND target.postal_code IS NOT DISTINCT FROM NULLIF(stage.postal_code, '')
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileContactsSQL = `
WITH upserted AS (
  INSERT INTO ariregister_source.contacts (
    company_id,
    raw_record_id,
    source_entry_id,
    contact_type,
    contact_type_label,
    contact_type_label_en,
    value,
    normalized_value,
    source,
    status,
    is_primary,
    ended_on,
    evidence,
    raw_contact_payload,
    metadata,
    last_seen_at,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    stage.contact_type,
    NULLIF(stage.contact_type_label, ''),
    NULLIF(stage.contact_type_label_en, ''),
    stage.value,
    stage.normalized_value,
    stage.source,
    stage.status,
    stage.is_primary,
    NULLIF(stage.ended_on, '')::date,
    stage.evidence::jsonb,
    stage.raw_contact_payload::jsonb,
    stage.metadata::jsonb,
    now(),
    now()
  FROM ariregister_source_contact_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
  ON CONFLICT (company_id, contact_type, normalized_value)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    source_entry_id = EXCLUDED.source_entry_id,
    contact_type_label = EXCLUDED.contact_type_label,
    contact_type_label_en = COALESCE(EXCLUDED.contact_type_label_en, ariregister_source.contacts.contact_type_label_en),
    value = EXCLUDED.value,
    source = EXCLUDED.source,
    status = EXCLUDED.status,
    is_primary = EXCLUDED.is_primary,
    ended_on = EXCLUDED.ended_on,
    evidence = EXCLUDED.evidence,
    raw_contact_payload = EXCLUDED.raw_contact_payload,
    metadata = ariregister_source.contacts.metadata || EXCLUDED.metadata,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileWebsitesSQL = `
WITH resolved AS (
  SELECT
    company.company_id,
    contact.id AS contact_id,
    stage.*
  FROM ariregister_source_website_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
  LEFT JOIN ariregister_source.contacts contact
    ON contact.company_id = company.company_id
   AND contact.contact_type = 'website'
   AND contact.normalized_value = stage.normalized_url
),
upserted AS (
  INSERT INTO ariregister_source.websites (
    company_id,
    raw_record_id,
    contact_id,
    url,
    normalized_url,
    host,
    path,
    website_type,
    source,
    status,
    confidence,
    is_primary,
    title,
    title_en,
    description,
    description_en,
    evidence,
    metadata,
    last_seen_at,
    updated_at
  )
  SELECT
    company_id,
    raw_record_id::uuid,
    contact_id,
    url,
    normalized_url,
    NULLIF(host, ''),
    NULLIF(path, ''),
    website_type,
    source,
    status,
    confidence,
    is_primary,
    NULLIF(title, ''),
    NULLIF(title_en, ''),
    NULLIF(description, ''),
    NULLIF(description_en, ''),
    evidence::jsonb,
    metadata::jsonb,
    now(),
    now()
  FROM resolved
  ON CONFLICT (company_id, normalized_url)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    contact_id = EXCLUDED.contact_id,
    url = EXCLUDED.url,
    host = EXCLUDED.host,
    path = EXCLUDED.path,
    website_type = EXCLUDED.website_type,
    source = EXCLUDED.source,
    status = EXCLUDED.status,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    title = EXCLUDED.title,
    title_en = COALESCE(EXCLUDED.title_en, ariregister_source.websites.title_en),
    description = EXCLUDED.description,
    description_en = COALESCE(EXCLUDED.description_en, ariregister_source.websites.description_en),
    evidence = EXCLUDED.evidence,
    metadata = ariregister_source.websites.metadata || EXCLUDED.metadata,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id, company_id, normalized_url
),
mapped AS (
  INSERT INTO ariregister_source_website_id_stage (registry_code, normalized_url, website_id)
  SELECT company.registry_code, upserted.normalized_url, upserted.id
  FROM upserted
  JOIN ariregister_source_company_id_stage company ON company.company_id = upserted.company_id
  ON CONFLICT (registry_code, normalized_url) DO UPDATE SET website_id = EXCLUDED.website_id
  RETURNING website_id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileDomainsSQL = `
WITH upserted AS (
  INSERT INTO ariregister_source.domains (
    company_id,
    raw_record_id,
    website_id,
    domain,
    normalized_domain,
    registrable_domain,
    domain_type,
    source,
    status,
    confidence,
    is_primary,
    best_signal,
    evidence,
    metadata,
    last_seen_at,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    website.website_id,
    stage.domain,
    stage.normalized_domain,
    stage.registrable_domain,
    stage.domain_type,
    stage.source,
    stage.status,
    stage.confidence,
    stage.is_primary,
    NULLIF(stage.best_signal, ''),
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now(),
    now()
  FROM ariregister_source_domain_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
  LEFT JOIN ariregister_source_website_id_stage website
    ON website.registry_code = stage.registry_code
   AND website.normalized_url = stage.website_normalized
  ON CONFLICT (company_id, normalized_domain)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    website_id = EXCLUDED.website_id,
    domain = EXCLUDED.domain,
    registrable_domain = EXCLUDED.registrable_domain,
    domain_type = EXCLUDED.domain_type,
    source = EXCLUDED.source,
    status = EXCLUDED.status,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    best_signal = EXCLUDED.best_signal,
    evidence = EXCLUDED.evidence,
    metadata = ariregister_source.domains.metadata || EXCLUDED.metadata,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileIndustriesSQL = `
WITH resolved AS (
  SELECT
    company.company_id,
    stage.*,
    nace_code.id AS nace_code_id
  FROM ariregister_source_industry_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
  LEFT JOIN nace_classifications nace_classification
    ON nace_classification.code_system = 'NACE'
   AND nace_classification.revision = NULLIF(stage.nace_revision, '')
  LEFT JOIN nace_codes nace_code
    ON nace_code.classification_id = nace_classification.id
   AND nace_code.code = stage.nace_code
   AND nace_code.active
),
upserted AS (
  INSERT INTO ariregister_source.industries (
    company_id,
    raw_record_id,
    nace_code_id,
    source_entry_id,
    classification_type,
    source_field,
    position,
    emtak_code,
    emtak_label,
    emtak_label_en,
    emtak_version,
    emtak_version_label,
    emtak_version_label_en,
    nace_code,
    nace_revision,
    nace_title,
    nace_title_en,
    mapping_method,
    mapping_confidence,
    is_primary,
    started_on,
    ended_on,
    raw_industry_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company_id,
    raw_record_id::uuid,
    nace_code_id,
    source_entry_id,
    classification_type,
    source_field,
    position,
    emtak_code,
    NULLIF(emtak_label, ''),
    NULLIF(emtak_label_en, ''),
    emtak_version,
    NULLIF(emtak_version_label, ''),
    NULLIF(emtak_version_label_en, ''),
    NULLIF(nace_code, ''),
    NULLIF(nace_revision, ''),
    NULLIF(nace_title, ''),
    NULLIF(nace_title_en, ''),
    NULLIF(mapping_method, ''),
    mapping_confidence,
    is_primary,
    NULLIF(started_on, '')::date,
    NULLIF(ended_on, '')::date,
    raw_industry_payload::jsonb,
    evidence::jsonb,
    metadata::jsonb,
    now()
  FROM resolved
  ON CONFLICT (company_id, classification_type, position)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    nace_code_id = EXCLUDED.nace_code_id,
    source_entry_id = EXCLUDED.source_entry_id,
    source_field = EXCLUDED.source_field,
    emtak_code = EXCLUDED.emtak_code,
    emtak_label = EXCLUDED.emtak_label,
    emtak_label_en = COALESCE(EXCLUDED.emtak_label_en, ariregister_source.industries.emtak_label_en),
    emtak_version = EXCLUDED.emtak_version,
    emtak_version_label = EXCLUDED.emtak_version_label,
    emtak_version_label_en = COALESCE(EXCLUDED.emtak_version_label_en, ariregister_source.industries.emtak_version_label_en),
    nace_code = EXCLUDED.nace_code,
    nace_revision = EXCLUDED.nace_revision,
    nace_title = EXCLUDED.nace_title,
    nace_title_en = COALESCE(EXCLUDED.nace_title_en, ariregister_source.industries.nace_title_en),
    mapping_method = EXCLUDED.mapping_method,
    mapping_confidence = EXCLUDED.mapping_confidence,
    is_primary = EXCLUDED.is_primary,
    started_on = EXCLUDED.started_on,
    ended_on = EXCLUDED.ended_on,
    raw_industry_payload = EXCLUDED.raw_industry_payload,
    evidence = EXCLUDED.evidence,
    metadata = ariregister_source.industries.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileCapitalSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_capital_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.capital target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    capital_amount = NULLIF(stage.capital_amount, '')::numeric,
    capital_currency = NULLIF(stage.capital_currency, ''),
    capital_currency_label = NULLIF(stage.capital_currency_label, ''),
    capital_currency_label_en = COALESCE(NULLIF(stage.capital_currency_label_en, ''), target.capital_currency_label_en),
    introduced_on = NULLIF(stage.introduced_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_capital_payload = stage.raw_capital_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.capital_amount IS NOT DISTINCT FROM NULLIF(stage.capital_amount, '')::numeric
        AND target.capital_currency IS NOT DISTINCT FROM NULLIF(stage.capital_currency, '')
        AND target.introduced_on IS NOT DISTINCT FROM NULLIF(stage.introduced_on, '')::date
        AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.capital (
    company_id,
    raw_record_id,
    source_entry_id,
    capital_amount,
    capital_currency,
    capital_currency_label,
    capital_currency_label_en,
    introduced_on,
    ended_on,
    raw_capital_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    NULLIF(stage.capital_amount, '')::numeric,
    NULLIF(stage.capital_currency, ''),
    NULLIF(stage.capital_currency_label, ''),
    NULLIF(stage.capital_currency_label_en, ''),
    NULLIF(stage.introduced_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_capital_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.capital target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.capital_amount IS NOT DISTINCT FROM NULLIF(stage.capital_amount, '')::numeric
          AND target.capital_currency IS NOT DISTINCT FROM NULLIF(stage.capital_currency, '')
          AND target.introduced_on IS NOT DISTINCT FROM NULLIF(stage.introduced_on, '')::date
          AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileFinancialYearPeriodsSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_financial_year_period_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.financial_year_periods target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    period_start_month_day = NULLIF(stage.period_start_month_day, ''),
    period_end_month_day = NULLIF(stage.period_end_month_day, ''),
    started_on = NULLIF(stage.started_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_period_payload = stage.raw_period_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.period_start_month_day IS NOT DISTINCT FROM NULLIF(stage.period_start_month_day, '')
        AND target.period_end_month_day IS NOT DISTINCT FROM NULLIF(stage.period_end_month_day, '')
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.financial_year_periods (
    company_id,
    raw_record_id,
    source_entry_id,
    period_start_month_day,
    period_end_month_day,
    started_on,
    ended_on,
    raw_period_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    NULLIF(stage.period_start_month_day, ''),
    NULLIF(stage.period_end_month_day, ''),
    NULLIF(stage.started_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_period_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.financial_year_periods target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.period_start_month_day IS NOT DISTINCT FROM NULLIF(stage.period_start_month_day, '')
          AND target.period_end_month_day IS NOT DISTINCT FROM NULLIF(stage.period_end_month_day, '')
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileAnnualReportsSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_annual_report_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.annual_reports target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    fiscal_year = stage.fiscal_year,
    period_start = NULLIF(stage.period_start, '')::date,
    period_end = NULLIF(stage.period_end, '')::date,
    employee_count = stage.employee_count,
    report_address = NULLIF(stage.report_address, ''),
    report_address_en = COALESCE(NULLIF(stage.report_address_en, ''), target.report_address_en),
    activity_emtak_code = NULLIF(stage.activity_emtak_code, ''),
    activity_label = NULLIF(stage.activity_label, ''),
    activity_label_en = COALESCE(NULLIF(stage.activity_label_en, ''), target.activity_label_en),
    activity_version = NULLIF(stage.activity_version, ''),
    activity_version_label = NULLIF(stage.activity_version_label, ''),
    activity_version_label_en = COALESCE(NULLIF(stage.activity_version_label_en, ''), target.activity_version_label_en),
    activity_nace_code = NULLIF(stage.activity_nace_code, ''),
    raw_report_payload = stage.raw_report_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.fiscal_year IS NOT DISTINCT FROM stage.fiscal_year
        AND target.period_start IS NOT DISTINCT FROM NULLIF(stage.period_start, '')::date
        AND target.period_end IS NOT DISTINCT FROM NULLIF(stage.period_end, '')::date
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.annual_reports (
    company_id,
    raw_record_id,
    source_entry_id,
    fiscal_year,
    period_start,
    period_end,
    employee_count,
    report_address,
    report_address_en,
    activity_emtak_code,
    activity_label,
    activity_label_en,
    activity_version,
    activity_version_label,
    activity_version_label_en,
    activity_nace_code,
    raw_report_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    stage.fiscal_year,
    NULLIF(stage.period_start, '')::date,
    NULLIF(stage.period_end, '')::date,
    stage.employee_count,
    NULLIF(stage.report_address, ''),
    NULLIF(stage.report_address_en, ''),
    NULLIF(stage.activity_emtak_code, ''),
    NULLIF(stage.activity_label, ''),
    NULLIF(stage.activity_label_en, ''),
    NULLIF(stage.activity_version, ''),
    NULLIF(stage.activity_version_label, ''),
    NULLIF(stage.activity_version_label_en, ''),
    NULLIF(stage.activity_nace_code, ''),
    stage.raw_report_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.annual_reports target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.fiscal_year IS NOT DISTINCT FROM stage.fiscal_year
          AND target.period_start IS NOT DISTINCT FROM NULLIF(stage.period_start, '')::date
          AND target.period_end IS NOT DISTINCT FROM NULLIF(stage.period_end, '')::date
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileArticlesSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_article_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.articles target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    confirmed_on = NULLIF(stage.confirmed_on, '')::date,
    changed_on = NULLIF(stage.changed_on, '')::date,
    explanation = NULLIF(stage.explanation, ''),
    explanation_en = COALESCE(NULLIF(stage.explanation_en, ''), target.explanation_en),
    contains_special_rights = stage.contains_special_rights,
    started_on = NULLIF(stage.started_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_articles_payload = stage.raw_articles_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.confirmed_on IS NOT DISTINCT FROM NULLIF(stage.confirmed_on, '')::date
        AND target.changed_on IS NOT DISTINCT FROM NULLIF(stage.changed_on, '')::date
        AND target.explanation IS NOT DISTINCT FROM NULLIF(stage.explanation, '')
        AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
        AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.articles (
    company_id,
    raw_record_id,
    source_entry_id,
    confirmed_on,
    changed_on,
    explanation,
    explanation_en,
    contains_special_rights,
    started_on,
    ended_on,
    raw_articles_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    NULLIF(stage.confirmed_on, '')::date,
    NULLIF(stage.changed_on, '')::date,
    NULLIF(stage.explanation, ''),
    NULLIF(stage.explanation_en, ''),
    stage.contains_special_rights,
    NULLIF(stage.started_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_articles_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.articles target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.confirmed_on IS NOT DISTINCT FROM NULLIF(stage.confirmed_on, '')::date
          AND target.changed_on IS NOT DISTINCT FROM NULLIF(stage.changed_on, '')::date
          AND target.explanation IS NOT DISTINCT FROM NULLIF(stage.explanation, '')
          AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
          AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`

const mergeSourceProfileRegistryNotesSQL = `
WITH resolved AS (
  SELECT company.company_id, stage.*
  FROM ariregister_source_registry_note_stage stage
  JOIN ariregister_source_company_id_stage company ON company.registry_code = stage.registry_code
),
updated AS (
  UPDATE ariregister_source.registry_notes target
  SET
    raw_record_id = stage.raw_record_id::uuid,
    card_region = stage.card_region,
    card_number = stage.card_number,
    card_type = NULLIF(stage.card_type, ''),
    entry_number = stage.entry_number,
    column_number = stage.column_number,
    note_type = NULLIF(stage.note_type, ''),
    note_type_label = NULLIF(stage.note_type_label, ''),
    note_type_label_en = COALESCE(NULLIF(stage.note_type_label_en, ''), target.note_type_label_en),
    note_text = NULLIF(stage.note_text, ''),
    note_text_en = COALESCE(NULLIF(stage.note_text_en, ''), target.note_text_en),
    started_on = NULLIF(stage.started_on, '')::date,
    ended_on = NULLIF(stage.ended_on, '')::date,
    raw_note_payload = stage.raw_note_payload::jsonb,
    evidence = stage.evidence::jsonb,
    metadata = target.metadata || stage.metadata::jsonb,
    updated_at = now()
  FROM resolved stage
  WHERE target.company_id = stage.company_id
    AND (
      (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
      OR (
        target.source_entry_id IS NULL
        AND stage.source_entry_id IS NULL
        AND target.note_type IS NOT DISTINCT FROM NULLIF(stage.note_type, '')
        AND target.note_text IS NOT DISTINCT FROM NULLIF(stage.note_text, '')
        AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
        AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
      )
    )
  RETURNING target.id
),
inserted AS (
  INSERT INTO ariregister_source.registry_notes (
    company_id,
    raw_record_id,
    source_entry_id,
    card_region,
    card_number,
    card_type,
    entry_number,
    column_number,
    note_type,
    note_type_label,
    note_type_label_en,
    note_text,
    note_text_en,
    started_on,
    ended_on,
    raw_note_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.source_entry_id,
    stage.card_region,
    stage.card_number,
    NULLIF(stage.card_type, ''),
    stage.entry_number,
    stage.column_number,
    NULLIF(stage.note_type, ''),
    NULLIF(stage.note_type_label, ''),
    NULLIF(stage.note_type_label_en, ''),
    NULLIF(stage.note_text, ''),
    NULLIF(stage.note_text_en, ''),
    NULLIF(stage.started_on, '')::date,
    NULLIF(stage.ended_on, '')::date,
    stage.raw_note_payload::jsonb,
    stage.evidence::jsonb,
    stage.metadata::jsonb,
    now()
  FROM resolved stage
  JOIN ariregister_source_company_id_stage company ON company.company_id = stage.company_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM ariregister_source.registry_notes target
    WHERE target.company_id = stage.company_id
      AND (
        (target.source_entry_id IS NOT NULL AND target.source_entry_id = stage.source_entry_id)
        OR (
          target.source_entry_id IS NULL
          AND stage.source_entry_id IS NULL
          AND target.note_type IS NOT DISTINCT FROM NULLIF(stage.note_type, '')
          AND target.note_text IS NOT DISTINCT FROM NULLIF(stage.note_text, '')
          AND target.started_on IS NOT DISTINCT FROM NULLIF(stage.started_on, '')::date
          AND target.ended_on IS NOT DISTINCT FROM NULLIF(stage.ended_on, '')::date
        )
      )
  )
  RETURNING id
)
SELECT ((SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted))::integer;
`
