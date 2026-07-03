CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.lv_companies
    ADD COLUMN IF NOT EXISTS vzd_address_text Nullable(String),
    ADD COLUMN IF NOT EXISTS vzd_address_postal_code Nullable(String),
    ADD COLUMN IF NOT EXISTS vzd_address_status LowCardinality(Nullable(String)),
    ADD COLUMN IF NOT EXISTS address_city_name Nullable(String),
    ADD COLUMN IF NOT EXISTS address_municipality_name Nullable(String),
    ADD COLUMN IF NOT EXISTS address_latitude Nullable(Float64),
    ADD COLUMN IF NOT EXISTS address_longitude Nullable(Float64);
