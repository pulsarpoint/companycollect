ALTER TABLE corpscout.companies_all
DROP COLUMN IF EXISTS signals_resolved_at;

ALTER TABLE corpscout.companies_all
DROP COLUMN IF EXISTS public_award_last_date;

ALTER TABLE corpscout.companies_all
DROP COLUMN IF EXISTS public_award_count;

ALTER TABLE corpscout.companies_all
DROP COLUMN IF EXISTS has_government_contract;
