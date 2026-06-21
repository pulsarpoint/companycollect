CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.company_website_domains
    ADD COLUMN IF NOT EXISTS domain_source LowCardinality(String) DEFAULT 'website' AFTER root_domain;