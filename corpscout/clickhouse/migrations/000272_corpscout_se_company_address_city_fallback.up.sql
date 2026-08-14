CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_address_geocode_results
    ADD COLUMN IF NOT EXISTS coordinate_locality Nullable(String)
        AFTER coordinate_method,
    ADD COLUMN IF NOT EXISTS coordinate_supporting_point_count UInt32
        AFTER coordinate_locality;
