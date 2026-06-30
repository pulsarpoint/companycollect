CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS last_submitted_accounts_year Nullable(String) AFTER primary_website_host;
