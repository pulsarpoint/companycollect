ALTER TABLE brreg_source.translation_queue_entries
  ADD COLUMN provider text NOT NULL DEFAULT 'default',
  ADD COLUMN model text NOT NULL DEFAULT '',
  ADD COLUMN prompt_version text NOT NULL DEFAULT 'v1',
  ADD COLUMN source_lang text NOT NULL DEFAULT 'no',
  ADD COLUMN target_lang text NOT NULL DEFAULT 'en';

ALTER TABLE ariregister_source.translation_queue_entries
  ADD COLUMN provider text NOT NULL DEFAULT 'default',
  ADD COLUMN model text NOT NULL DEFAULT '',
  ADD COLUMN prompt_version text NOT NULL DEFAULT 'v1',
  ADD COLUMN source_lang text NOT NULL DEFAULT 'et',
  ADD COLUMN target_lang text NOT NULL DEFAULT 'en';

CREATE INDEX idx_brreg_source_translation_queue_pending_config
  ON brreg_source.translation_queue_entries(status, provider, model, prompt_version, source_lang, target_lang, status_changed_at, company_id)
  WHERE status = 'pending';

CREATE INDEX idx_ariregister_source_translation_queue_pending_config
  ON ariregister_source.translation_queue_entries(status, provider, model, prompt_version, source_lang, target_lang, status_changed_at, company_id)
  WHERE status = 'pending';
