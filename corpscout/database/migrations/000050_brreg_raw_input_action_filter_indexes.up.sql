CREATE INDEX idx_brreg_raw_input_actions_type_raw_payload_attempt
  ON brreg_raw_input_actions (action_type, raw_input_id, payload_hash, attempt DESC, created_at DESC);

CREATE INDEX idx_brreg_raw_input_action_events_action_status_latest
  ON brreg_raw_input_action_events (action_id, status, created_at DESC);
