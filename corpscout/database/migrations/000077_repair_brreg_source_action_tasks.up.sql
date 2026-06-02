CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS brreg_source.action_tasks (
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

CREATE INDEX IF NOT EXISTS idx_brreg_source_action_queue
  ON brreg_source.action_tasks(action_type, status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX IF NOT EXISTS idx_brreg_source_action_company
  ON brreg_source.action_tasks(company_id, action_type, status);

DO $$
BEGIN
  IF to_regclass('brreg_source.field_translation_tasks') IS NOT NULL THEN
    EXECUTE $migrate$
      INSERT INTO brreg_source.action_tasks (
        id,
        company_id,
        action_type,
        source_table,
        source_row_id,
        target_key,
        source_fingerprint,
        source_column,
        target_column,
        source_text,
        source_lang,
        target_lang,
        status,
        attempt_count,
        max_attempts,
        lease_until,
        last_started_at,
        last_finished_at,
        result,
        model,
        prompt_version,
        error,
        error_category,
        error_code,
        retry_strategy,
        metadata,
        created_at,
        updated_at
      )
      SELECT
        old_task.id,
        old_task.company_id,
        'translate_field',
        old_task.source_table,
        old_task.source_row_id,
        old_task.target_column,
        old_task.source_text_hash,
        old_task.source_column,
        old_task.target_column,
        old_task.source_text,
        old_task.source_lang,
        old_task.target_lang,
        old_task.status,
        old_task.attempt_count,
        old_task.max_attempts,
        old_task.lease_until,
        old_task.last_started_at,
        old_task.last_finished_at,
        CASE
          WHEN NULLIF(btrim(old_task.translated_text), '') IS NULL THEN '{}'::jsonb
          ELSE jsonb_build_object('translated_text', old_task.translated_text)
        END,
        old_task.model,
        old_task.prompt_version,
        old_task.error,
        old_task.error_category,
        old_task.error_code,
        old_task.retry_strategy,
        old_task.metadata,
        old_task.created_at,
        old_task.updated_at
      FROM brreg_source.field_translation_tasks old_task
      ON CONFLICT (action_type, source_table, source_row_id, target_key, source_fingerprint) DO NOTHING
    $migrate$;
  END IF;
END $$;

DROP VIEW IF EXISTS brreg_source.v_company_detail;
DROP VIEW IF EXISTS brreg_source.v_company_explorer;

CREATE VIEW brreg_source.v_company_explorer AS
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

CREATE VIEW brreg_source.v_company_detail AS
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

GRANT SELECT ON ALL TABLES IN SCHEMA brreg_source TO corpscout_anon;
