CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_addresses_canonical_current
    ADD COLUMN IF NOT EXISTS street_name String AFTER street_address;

ALTER TABLE corpscout.se_company_addresses_canonical_current
    ADD COLUMN IF NOT EXISTS house_number String AFTER street_name;

ALTER TABLE corpscout.se_company_addresses_canonical_current
    ADD COLUMN IF NOT EXISTS unit String AFTER house_number;

ALTER TABLE corpscout.se_company_address_members_current
    ADD COLUMN IF NOT EXISTS street_name String AFTER street_address;

ALTER TABLE corpscout.se_company_address_members_current
    ADD COLUMN IF NOT EXISTS house_number String AFTER street_name;

ALTER TABLE corpscout.se_company_address_members_current
    ADD COLUMN IF NOT EXISTS unit String AFTER house_number;

ALTER TABLE corpscout.se_addresses_current
    ADD COLUMN IF NOT EXISTS street_name String AFTER street_address;

ALTER TABLE corpscout.se_addresses_current
    ADD COLUMN IF NOT EXISTS house_number String AFTER street_name;

ALTER TABLE corpscout.se_addresses_current
    ADD COLUMN IF NOT EXISTS unit String AFTER house_number;
