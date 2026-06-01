ALTER TABLE brreg_workflow.raw_records
  ADD COLUMN IF NOT EXISTS corpscout_raw_input_id UUID REFERENCES brreg_company_raw_inputs(id) ON DELETE SET NULL;
