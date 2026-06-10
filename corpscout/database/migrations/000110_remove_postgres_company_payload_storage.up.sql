DROP SCHEMA IF EXISTS brreg_source CASCADE;
DROP SCHEMA IF EXISTS brreg_workflow CASCADE;
DROP SCHEMA IF EXISTS ariregister_source CASCADE;
DROP SCHEMA IF EXISTS ariregister_workflow CASCADE;
DROP SCHEMA IF EXISTS cvr_workflow CASCADE;
DROP SCHEMA IF EXISTS france_source CASCADE;
DROP SCHEMA IF EXISTS france_workflow CASCADE;
DROP SCHEMA IF EXISTS se_workflow CASCADE;
DROP SCHEMA IF EXISTS source_translation CASCADE;
DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_colorado_entities CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_irs_eo_bmf CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_sam_gov_entity CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_sec_edgar CASCADE;

DROP VIEW IF EXISTS v_company_locations;
DROP VIEW IF EXISTS v_company_phones;
DROP VIEW IF EXISTS v_company_emails;
DROP VIEW IF EXISTS v_company_industries;
DROP VIEW IF EXISTS v_company_markets;
DROP VIEW IF EXISTS v_company_services;

DROP TABLE IF EXISTS suggestion_company_relationships CASCADE;
DROP TABLE IF EXISTS suggestion_company_services CASCADE;
DROP TABLE IF EXISTS suggestion_company_markets CASCADE;
DROP TABLE IF EXISTS suggestion_company_industries CASCADE;
DROP TABLE IF EXISTS suggestion_company_financials CASCADE;
DROP TABLE IF EXISTS suggestion_company_phones CASCADE;
DROP TABLE IF EXISTS suggestion_company_emails CASCADE;
DROP TABLE IF EXISTS suggestion_company_locations CASCADE;
DROP TABLE IF EXISTS suggestion_company_domains CASCADE;
DROP TABLE IF EXISTS suggestion_company_profiles CASCADE;
DROP TABLE IF EXISTS suggestions CASCADE;

DROP TABLE IF EXISTS company_relationship_suggestions CASCADE;
DROP TABLE IF EXISTS company_status_suggestions CASCADE;
DROP TABLE IF EXISTS company_location_suggestions CASCADE;
DROP TABLE IF EXISTS company_contact_suggestions CASCADE;
DROP TABLE IF EXISTS company_domain_suggestions CASCADE;
DROP TABLE IF EXISTS open_source_project_suggestions CASCADE;
DROP TABLE IF EXISTS organization_suggestions CASCADE;
DROP TABLE IF EXISTS company_suggestions CASCADE;
DROP TABLE IF EXISTS suggestion_source_links CASCADE;

DROP TABLE IF EXISTS company_financials CASCADE;
DROP TABLE IF EXISTS company_addresses CASCADE;
DROP TABLE IF EXISTS company_services CASCADE;
DROP TABLE IF EXISTS company_markets CASCADE;
DROP TABLE IF EXISTS company_industries CASCADE;
DROP TABLE IF EXISTS company_emails CASCADE;
DROP TABLE IF EXISTS company_phones CASCADE;
DROP TABLE IF EXISTS company_locations CASCADE;

DROP TABLE IF EXISTS domain_import_batches CASCADE;
DROP TABLE IF EXISTS domain_crawl_job_pages CASCADE;
DROP TABLE IF EXISTS domain_crawl_jobs CASCADE;

DROP TABLE IF EXISTS brreg_source_financials CASCADE;
DROP TABLE IF EXISTS brreg_source_domains CASCADE;
DROP TABLE IF EXISTS brreg_source_capital CASCADE;
DROP TABLE IF EXISTS brreg_source_industries CASCADE;
DROP TABLE IF EXISTS brreg_source_addresses CASCADE;
DROP TABLE IF EXISTS brreg_source_companies CASCADE;
DROP TABLE IF EXISTS brreg_enhanced_raw_inputs CASCADE;
DROP TABLE IF EXISTS brreg_raw_input_domains CASCADE;
DROP TABLE IF EXISTS brreg_raw_input_action_events CASCADE;
DROP TABLE IF EXISTS brreg_raw_input_actions CASCADE;

DROP TABLE IF EXISTS ariregister_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS cvr_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS domain_discovery_raw_inputs CASCADE;
DROP TABLE IF EXISTS ai_company_profile_raw_inputs CASCADE;
DROP TABLE IF EXISTS brreg_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS companies_house_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS gleif_company_raw_inputs CASCADE;

DROP TABLE IF EXISTS translation_bindings CASCADE;
DROP TABLE IF EXISTS translation_cache CASCADE;
