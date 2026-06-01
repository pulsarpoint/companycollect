DROP VIEW IF EXISTS v_source_raw_inputs;

ALTER TABLE brreg_workflow.enhanced_records
  DROP CONSTRAINT IF EXISTS enhanced_records_corpscout_source_company_id_fkey;

DROP TABLE IF EXISTS brreg_source_financials;
DROP TABLE IF EXISTS brreg_source_domains;
DROP TABLE IF EXISTS brreg_source_capital;
DROP TABLE IF EXISTS brreg_source_industries;
DROP TABLE IF EXISTS brreg_source_addresses;
DROP TABLE IF EXISTS brreg_source_companies;

ALTER TABLE IF EXISTS legacy_brreg_company_raw_inputs
  RENAME TO brreg_company_raw_inputs;

ALTER TABLE IF EXISTS legacy_brreg_raw_input_actions
  RENAME TO brreg_raw_input_actions;

ALTER TABLE IF EXISTS legacy_brreg_raw_input_action_events
  RENAME TO brreg_raw_input_action_events;

ALTER TABLE IF EXISTS legacy_brreg_raw_input_domains
  RENAME TO brreg_raw_input_domains;

ALTER TABLE IF EXISTS legacy_brreg_enhanced_raw_inputs
  RENAME TO brreg_enhanced_raw_inputs;

ALTER TABLE brreg_workflow.enhanced_records
  ADD COLUMN IF NOT EXISTS corpscout_enhanced_raw_input_id UUID REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE SET NULL;
