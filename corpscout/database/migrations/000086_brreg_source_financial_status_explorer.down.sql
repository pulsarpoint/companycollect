DROP MATERIALIZED VIEW IF EXISTS brreg_source.mv_company_explorer;

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
    count(*) FILTER (WHERE action_type = 'translate_field' AND status IN ('failed_retryable', 'failed_terminal'))::bigint AS translation_failed_count,
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
  coalesce(action_counts.translation_failed_count, 0) AS translation_failed_count,
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

CREATE MATERIALIZED VIEW brreg_source.mv_company_explorer AS
SELECT *
FROM brreg_source.v_company_explorer;

CREATE UNIQUE INDEX uq_brreg_source_mv_company_explorer_company
  ON brreg_source.mv_company_explorer(company_id);

CREATE INDEX idx_brreg_source_mv_company_explorer_updated
  ON brreg_source.mv_company_explorer(updated_at DESC, organization_number ASC);

CREATE INDEX idx_brreg_source_mv_company_explorer_lifecycle_updated
  ON brreg_source.mv_company_explorer(lifecycle_status, updated_at DESC, organization_number ASC);

CREATE INDEX idx_brreg_source_mv_company_explorer_registration_updated
  ON brreg_source.mv_company_explorer(registration_status, updated_at DESC, organization_number ASC)
  WHERE registration_status IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_with_website
  ON brreg_source.mv_company_explorer(updated_at DESC, organization_number ASC)
  WHERE website_count > 0;

CREATE INDEX idx_brreg_source_mv_company_explorer_without_website
  ON brreg_source.mv_company_explorer(updated_at DESC, organization_number ASC)
  WHERE website_count = 0;

CREATE INDEX idx_brreg_source_mv_company_explorer_missing_translation
  ON brreg_source.mv_company_explorer(updated_at DESC, organization_number ASC)
  WHERE translation_missing_count > 0;

CREATE INDEX idx_brreg_source_mv_company_explorer_complete_translation
  ON brreg_source.mv_company_explorer(updated_at DESC, organization_number ASC)
  WHERE translation_missing_count = 0;

CREATE INDEX idx_brreg_source_mv_company_explorer_org_name_trgm
  ON brreg_source.mv_company_explorer USING gin (organization_name gin_trgm_ops);

CREATE INDEX idx_brreg_source_mv_company_explorer_org_number_trgm
  ON brreg_source.mv_company_explorer USING gin (organization_number gin_trgm_ops);

CREATE INDEX idx_brreg_source_mv_company_explorer_industry_trgm
  ON brreg_source.mv_company_explorer USING gin (primary_industry_label gin_trgm_ops)
  WHERE primary_industry_label IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_city_trgm
  ON brreg_source.mv_company_explorer USING gin (city gin_trgm_ops)
  WHERE city IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_municipality_trgm
  ON brreg_source.mv_company_explorer USING gin (municipality gin_trgm_ops)
  WHERE municipality IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_organization_sort
  ON brreg_source.mv_company_explorer(organization_name, organization_number);

CREATE INDEX idx_brreg_source_mv_company_explorer_industry_sort
  ON brreg_source.mv_company_explorer(primary_industry_label, organization_number)
  WHERE primary_industry_label IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_location_sort
  ON brreg_source.mv_company_explorer(city, organization_number)
  WHERE city IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_employee_sort
  ON brreg_source.mv_company_explorer(employee_count, organization_number)
  WHERE employee_count IS NOT NULL;

CREATE INDEX idx_brreg_source_mv_company_explorer_revenue_sort
  ON brreg_source.mv_company_explorer(latest_revenue_usd_cents, organization_number)
  WHERE latest_revenue_usd_cents IS NOT NULL;

GRANT SELECT ON brreg_source.mv_company_explorer TO corpscout_anon;
