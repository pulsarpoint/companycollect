ALTER TABLE corpscout.se_address_geocodes_current
    DROP COLUMN IF EXISTS coordinate_spread_meters;

ALTER TABLE corpscout.se_company_address_geocode_results
    DROP COLUMN IF EXISTS coordinate_spread_meters;
