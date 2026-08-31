CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_info_esef
    DROP COLUMN IF EXISTS material_group_relationships_json,
    DROP COLUMN IF EXISTS operating_geographies_json,
    DROP COLUMN IF EXISTS customer_markets_json;
