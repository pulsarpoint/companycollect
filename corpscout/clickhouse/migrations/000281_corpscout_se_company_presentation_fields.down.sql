ALTER TABLE corpscout.se_companies
    DROP COLUMN IF EXISTS status_conflict,
    DROP COLUMN IF EXISTS status_observed_at,
    DROP COLUMN IF EXISTS status_source,
    DROP COLUMN IF EXISTS legal_name_registration_date;
