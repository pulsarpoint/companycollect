CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_companies
    ADD COLUMN IF NOT EXISTS legal_name_registration_date Nullable(Date32)
        AFTER legal_name_raw,
    ADD COLUMN IF NOT EXISTS status_source LowCardinality(Nullable(String))
        AFTER status,
    ADD COLUMN IF NOT EXISTS status_observed_at Nullable(DateTime64(3, 'UTC'))
        AFTER status_source,
    ADD COLUMN IF NOT EXISTS status_conflict UInt8 DEFAULT 0
        AFTER status_observed_at;
