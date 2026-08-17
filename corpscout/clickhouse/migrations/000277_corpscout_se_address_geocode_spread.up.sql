CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_address_geocode_results
    ADD COLUMN IF NOT EXISTS coordinate_spread_meters Nullable(Float64)
        AFTER coordinate_supporting_point_count;

ALTER TABLE corpscout.se_address_geocodes_current
    ADD COLUMN IF NOT EXISTS coordinate_spread_meters Nullable(Float64)
        AFTER coordinate_supporting_point_count;
