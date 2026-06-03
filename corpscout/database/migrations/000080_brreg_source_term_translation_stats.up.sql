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
translation_fields AS (
  SELECT
    company.id AS company_id,
    'no'::text AS source_lang,
    'en'::text AS target_lang,
    encode(digest(lower(btrim(company.organization_form_label)), 'sha256'), 'hex') AS term_key
  FROM brreg_source.companies company
  WHERE company.row_status = 'active'
    AND nullif(btrim(company.organization_form_label), '') IS NOT NULL
  UNION ALL
  SELECT company.id, 'no', 'en', encode(digest(lower(btrim(company.response_class)), 'sha256'), 'hex')
  FROM brreg_source.companies company
  WHERE company.row_status = 'active'
    AND nullif(btrim(company.response_class), '') IS NOT NULL
  UNION ALL
  SELECT company.id, 'no', 'en', encode(digest(lower(btrim(company.activity_description)), 'sha256'), 'hex')
  FROM brreg_source.companies company
  WHERE company.row_status = 'active'
    AND nullif(btrim(company.activity_description), '') IS NOT NULL
  UNION ALL
  SELECT company.id, 'no', 'en', encode(digest(lower(btrim(company.statutory_purpose)), 'sha256'), 'hex')
  FROM brreg_source.companies company
  WHERE company.row_status = 'active'
    AND nullif(btrim(company.statutory_purpose), '') IS NOT NULL
  UNION ALL
  SELECT capital.company_id, 'no', 'en', encode(digest(lower(btrim(capital.capital_type)), 'sha256'), 'hex')
  FROM brreg_source.capital capital
  JOIN brreg_source.companies company ON company.id = capital.company_id
  WHERE company.row_status = 'active'
    AND nullif(btrim(capital.capital_type), '') IS NOT NULL
),
missing_translation_counts AS (
  SELECT company_id, count(*)::bigint AS translation_missing_count
  FROM brreg_source.v_missing_translation_fields
  GROUP BY company_id
),
latest_term_status AS (
  SELECT DISTINCT ON (source_lang, target_lang, term_key)
    source_lang,
    target_lang,
    term_key,
    status
  FROM brreg_source.translation_terms
  WHERE source = 'brreg'
  ORDER BY source_lang, target_lang, term_key, updated_at DESC, created_at DESC, id DESC
),
term_status AS (
  SELECT
    source_lang,
    target_lang,
    term_key,
    CASE
      WHEN status = 'succeeded' THEN 'succeeded'
      WHEN status IN ('pending', 'queued') THEN 'pending'
      WHEN status IN ('failed_retryable', 'failed_terminal') THEN 'failed'
      ELSE 'missing'
    END AS effective_status
  FROM latest_term_status
),
missing_term_counts AS (
  SELECT
    missing.company_id,
    count(DISTINCT missing.term_key) FILTER (WHERE term_status.effective_status = 'pending')::bigint AS translation_pending_count,
    count(DISTINCT missing.term_key) FILTER (WHERE term_status.effective_status = 'failed')::bigint AS translation_failed_count
  FROM brreg_source.v_missing_translation_fields missing
  LEFT JOIN term_status
    ON term_status.source_lang = missing.source_lang
   AND term_status.target_lang = missing.target_lang
   AND term_status.term_key = missing.term_key
  GROUP BY missing.company_id
),
succeeded_term_counts AS (
  SELECT
    field.company_id,
    count(DISTINCT field.term_key) FILTER (WHERE term_status.effective_status = 'succeeded')::bigint AS translation_succeeded_count
  FROM translation_fields field
  LEFT JOIN term_status
    ON term_status.source_lang = field.source_lang
   AND term_status.target_lang = field.target_lang
   AND term_status.term_key = field.term_key
  GROUP BY field.company_id
),
action_counts AS (
  SELECT
    company_id,
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
  coalesce(missing_translation_counts.translation_missing_count, 0) AS translation_missing_count,
  coalesce(missing_term_counts.translation_pending_count, 0) AS translation_pending_count,
  0::bigint AS translation_running_count,
  coalesce(succeeded_term_counts.translation_succeeded_count, 0) AS translation_succeeded_count,
  coalesce(action_counts.domain_pending_count, 0) AS domain_pending_count,
  coalesce(action_counts.domain_running_count, 0) AS domain_running_count,
  coalesce(action_counts.domain_succeeded_count, 0) AS domain_succeeded_count,
  company.updated_at,
  coalesce(missing_term_counts.translation_failed_count, 0) AS translation_failed_count
FROM brreg_source.companies company
LEFT JOIN primary_address address ON address.company_id = company.id
LEFT JOIN primary_industry industry ON industry.company_id = company.id
LEFT JOIN latest_financial ON latest_financial.company_id = company.id
LEFT JOIN website_counts ON website_counts.company_id = company.id
LEFT JOIN domain_counts ON domain_counts.company_id = company.id
LEFT JOIN contact_counts ON contact_counts.company_id = company.id
LEFT JOIN missing_translation_counts ON missing_translation_counts.company_id = company.id
LEFT JOIN missing_term_counts ON missing_term_counts.company_id = company.id
LEFT JOIN succeeded_term_counts ON succeeded_term_counts.company_id = company.id
LEFT JOIN action_counts ON action_counts.company_id = company.id
WHERE company.row_status = 'active';

GRANT SELECT ON brreg_source.v_company_explorer TO corpscout_anon;
