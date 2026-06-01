-- Allow Temporal pipeline writes without a legacy source_pull_run_id.
-- The Temporal worker creates rows directly and has no source_pull_runs entry.
ALTER TABLE companies_house_company_raw_inputs ALTER COLUMN source_pull_run_id DROP NOT NULL;
ALTER TABLE brreg_company_raw_inputs           ALTER COLUMN source_pull_run_id DROP NOT NULL;
