DROP TABLE IF EXISTS corpscout.domains;
DROP TABLE IF EXISTS corpscout.country_domains;
DROP TABLE IF EXISTS corpscout.company_website_domains;
DROP TABLE IF EXISTS corpscout.no_financial_statements;
DROP TABLE IF EXISTS corpscout.no_industries;
DROP TABLE IF EXISTS corpscout.no_websites;
DROP TABLE IF EXISTS corpscout.no_companies;

ALTER TABLE corpscout.fi_websites
    DROP COLUMN IF EXISTS root_domain;
