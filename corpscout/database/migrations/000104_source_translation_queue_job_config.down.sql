DROP INDEX IF EXISTS ariregister_source.idx_ariregister_source_translation_queue_pending_config;
DROP INDEX IF EXISTS brreg_source.idx_brreg_source_translation_queue_pending_config;

ALTER TABLE ariregister_source.translation_queue_entries
  DROP COLUMN IF EXISTS target_lang,
  DROP COLUMN IF EXISTS source_lang,
  DROP COLUMN IF EXISTS prompt_version,
  DROP COLUMN IF EXISTS model,
  DROP COLUMN IF EXISTS provider;

ALTER TABLE brreg_source.translation_queue_entries
  DROP COLUMN IF EXISTS target_lang,
  DROP COLUMN IF EXISTS source_lang,
  DROP COLUMN IF EXISTS prompt_version,
  DROP COLUMN IF EXISTS model,
  DROP COLUMN IF EXISTS provider;
