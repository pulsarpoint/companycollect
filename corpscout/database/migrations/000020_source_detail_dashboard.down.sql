REVOKE SELECT ON suggestion_source_links FROM corpscout_anon;
REVOKE SELECT ON domain_discovery_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON ai_company_profile_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON brreg_company_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON companies_house_company_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON gleif_company_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON v_source_raw_inputs FROM corpscout_anon;

DROP VIEW IF EXISTS v_source_raw_inputs;

ALTER TABLE data_sources
    DROP COLUMN IF EXISTS schedule_enabled;
