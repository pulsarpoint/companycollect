-- database/migrations/000001_initial_schema.down.sql
DROP TABLE IF EXISTS company_domain_reviews;
DROP TABLE IF EXISTS company_domains;
DROP TABLE IF EXISTS domains;
DROP TABLE IF EXISTS company_sources;
DROP TABLE IF EXISTS company_aliases;
DROP INDEX  IF EXISTS companies_country_reg_uniq;
DROP TABLE  IF EXISTS companies;
DROP TABLE  IF EXISTS source_snapshots;
DROP TABLE  IF EXISTS source_pull_runs;
DROP TABLE  IF EXISTS data_sources;
DROP TABLE  IF EXISTS countries;
