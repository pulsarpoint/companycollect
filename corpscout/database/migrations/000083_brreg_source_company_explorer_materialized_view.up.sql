CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP MATERIALIZED VIEW IF EXISTS brreg_source.mv_company_explorer;

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
