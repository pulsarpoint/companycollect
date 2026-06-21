CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.fr_companies
    ADD COLUMN IF NOT EXISTS address String,
    ADD COLUMN IF NOT EXISTS address_supplement String,
    ADD COLUMN IF NOT EXISTS postal_code LowCardinality(String),
    ADD COLUMN IF NOT EXISTS city String,
    ADD COLUMN IF NOT EXISTS city_code LowCardinality(String),
    ADD COLUMN IF NOT EXISTS country_label LowCardinality(String);
