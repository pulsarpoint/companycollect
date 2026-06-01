-- name: GetStats :one
SELECT
  (SELECT COUNT(*) FROM companies)::bigint                                     AS total_companies,
  (SELECT COUNT(*) FROM domains)::bigint                                       AS total_domains,
  (SELECT COUNT(*) FROM company_domains WHERE status = 'active')::bigint       AS active_domains,
  (SELECT COUNT(*) FROM company_domains WHERE status = 'needs_review')::bigint AS pending_review,
  (SELECT COUNT(*) FROM data_sources WHERE enabled = true)::bigint             AS enabled_sources,
  (
    (SELECT COUNT(*) FROM companies_house_company_raw_inputs WHERE processing_status = 'pending') +
    COALESCE((SELECT artifact_missing FROM brreg_workflow.v_enhanced_asset_state LIMIT 1), 0)
  )::bigint AS pending_raw_inputs;
