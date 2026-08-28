CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires the consolidated serving view. se_companies_current (migration 000326) is untouched
-- by this pair, so a rollback leaves the previous serving surface exactly where it was.
DROP VIEW IF EXISTS corpscout.se_companies_serving;
