ALTER TABLE corpscout.se_addresses_current
    DROP COLUMN IF EXISTS unit;

ALTER TABLE corpscout.se_addresses_current
    DROP COLUMN IF EXISTS house_number;

ALTER TABLE corpscout.se_addresses_current
    DROP COLUMN IF EXISTS street_name;

ALTER TABLE corpscout.se_company_address_members_current
    DROP COLUMN IF EXISTS unit;

ALTER TABLE corpscout.se_company_address_members_current
    DROP COLUMN IF EXISTS house_number;

ALTER TABLE corpscout.se_company_address_members_current
    DROP COLUMN IF EXISTS street_name;

ALTER TABLE corpscout.se_company_addresses_canonical_current
    DROP COLUMN IF EXISTS unit;

ALTER TABLE corpscout.se_company_addresses_canonical_current
    DROP COLUMN IF EXISTS house_number;

ALTER TABLE corpscout.se_company_addresses_canonical_current
    DROP COLUMN IF EXISTS street_name;
