ALTER TABLE corpscout.lv_companies
    DROP COLUMN IF EXISTS address_longitude,
    DROP COLUMN IF EXISTS address_latitude,
    DROP COLUMN IF EXISTS address_municipality_name,
    DROP COLUMN IF EXISTS address_city_name,
    DROP COLUMN IF EXISTS vzd_address_status,
    DROP COLUMN IF EXISTS vzd_address_postal_code,
    DROP COLUMN IF EXISTS vzd_address_text;
