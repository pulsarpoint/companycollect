CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.ee_company_contacts
    ADD COLUMN IF NOT EXISTS domain String,
    ADD COLUMN IF NOT EXISTS domain_source LowCardinality(String);