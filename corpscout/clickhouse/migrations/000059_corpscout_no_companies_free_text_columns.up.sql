CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS company_description_original Nullable(String);
ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS articles_purpose_original Nullable(String);
ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS activity_text_original Nullable(String);
