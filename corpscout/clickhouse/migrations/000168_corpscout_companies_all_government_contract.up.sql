CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.companies_all
ADD COLUMN IF NOT EXISTS has_government_contract UInt8 DEFAULT 0
AFTER has_financials;

ALTER TABLE corpscout.companies_all
ADD COLUMN IF NOT EXISTS public_award_count Nullable(UInt32)
AFTER has_government_contract;

ALTER TABLE corpscout.companies_all
ADD COLUMN IF NOT EXISTS public_award_last_date Nullable(Date)
AFTER public_award_count;

ALTER TABLE corpscout.companies_all
ADD COLUMN IF NOT EXISTS signals_resolved_at DateTime64(3, 'UTC') DEFAULT now64(3)
AFTER public_award_last_date;
