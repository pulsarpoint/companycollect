CREATE INDEX idx_brreg_raw_inputs_created_at
  ON brreg_company_raw_inputs (created_at DESC);

CREATE INDEX idx_brreg_raw_input_actions_enhance_lookup
  ON brreg_raw_input_actions (raw_input_id, payload_hash, attempt DESC, created_at DESC)
  WHERE action_type = 'enhance';
