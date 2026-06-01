DROP INDEX IF EXISTS idx_ch_raw_inputs_run_id;
DROP INDEX IF EXISTS idx_gleif_raw_inputs_run_id;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_run_id;

ALTER TABLE companies_house_company_raw_inputs DROP COLUMN IF EXISTS run_id;
ALTER TABLE gleif_company_raw_inputs           DROP COLUMN IF EXISTS run_id;
ALTER TABLE brreg_company_raw_inputs           DROP COLUMN IF EXISTS run_id;

DROP INDEX IF EXISTS idx_temporal_executions_status;
DROP INDEX IF EXISTS idx_temporal_executions_source;
DROP TABLE IF EXISTS temporal_executions;
