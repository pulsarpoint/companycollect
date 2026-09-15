ALTER TABLE corpscout.se_company_person_match_state
    DROP COLUMN IF EXISTS config_snapshot,
    DROP COLUMN IF EXISTS input_snapshot,
    DROP COLUMN IF EXISTS model_hash,
    DROP COLUMN IF EXISTS prompt_hash,
    DROP COLUMN IF EXISTS bindings_hash,
    DROP COLUMN IF EXISTS data_hash;

DROP TABLE IF EXISTS corpscout.se_company_person_match_input;
