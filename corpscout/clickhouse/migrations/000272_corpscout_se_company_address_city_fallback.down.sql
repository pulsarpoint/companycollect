ALTER TABLE corpscout.se_company_address_geocode_results
    DROP COLUMN IF EXISTS coordinate_supporting_point_count,
    DROP COLUMN IF EXISTS coordinate_locality;
