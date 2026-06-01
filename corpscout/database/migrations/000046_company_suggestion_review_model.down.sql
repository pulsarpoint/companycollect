DROP TABLE IF EXISTS suggestion_company_relationships;
DROP TABLE IF EXISTS suggestion_company_services;
DROP TABLE IF EXISTS suggestion_company_markets;
DROP TABLE IF EXISTS suggestion_company_industries;
DROP TABLE IF EXISTS suggestion_company_financials;
DROP TABLE IF EXISTS suggestion_company_phones;
DROP TABLE IF EXISTS suggestion_company_emails;
DROP TABLE IF EXISTS suggestion_company_locations;
DROP TABLE IF EXISTS suggestion_company_domains;
DROP TABLE IF EXISTS suggestion_company_profiles;
DROP TABLE IF EXISTS suggestions;

ALTER TABLE data_sources
    DROP CONSTRAINT IF EXISTS uq_data_sources_id_name;
