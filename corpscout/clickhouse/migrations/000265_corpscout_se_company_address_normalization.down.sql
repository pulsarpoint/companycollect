CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_addresses_current
    DROP COLUMN IF EXISTS normalized_address;

ALTER TABLE corpscout.se_company_addresses
    DROP COLUMN IF EXISTS normalized_address;
