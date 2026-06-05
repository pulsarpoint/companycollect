CREATE TABLE brreg_source.translation_queue_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  status text NOT NULL,
  num_of_characters integer NOT NULL,
  batch_id text,
  status_changed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_translation_queue_status
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  CONSTRAINT chk_brreg_source_translation_queue_chars
    CHECK (num_of_characters >= 0),
  CONSTRAINT chk_brreg_source_translation_queue_batch
    CHECK (
      (status = 'running' AND batch_id IS NOT NULL)
      OR status <> 'running'
    )
);

CREATE UNIQUE INDEX uq_brreg_source_translation_queue_active_company
  ON brreg_source.translation_queue_entries(company_id)
  WHERE status IN ('pending', 'running');

CREATE INDEX idx_brreg_source_translation_queue_pending
  ON brreg_source.translation_queue_entries(status, status_changed_at, company_id)
  WHERE status = 'pending';

CREATE INDEX idx_brreg_source_translation_queue_running
  ON brreg_source.translation_queue_entries(status, status_changed_at)
  WHERE status = 'running';

CREATE INDEX idx_brreg_source_translation_queue_batch
  ON brreg_source.translation_queue_entries(batch_id)
  WHERE batch_id IS NOT NULL;

CREATE TABLE ariregister_source.translation_queue_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  status text NOT NULL,
  num_of_characters integer NOT NULL,
  batch_id text,
  status_changed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_translation_queue_status
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  CONSTRAINT chk_ariregister_source_translation_queue_chars
    CHECK (num_of_characters >= 0),
  CONSTRAINT chk_ariregister_source_translation_queue_batch
    CHECK (
      (status = 'running' AND batch_id IS NOT NULL)
      OR status <> 'running'
    )
);

CREATE UNIQUE INDEX uq_ariregister_source_translation_queue_active_company
  ON ariregister_source.translation_queue_entries(company_id)
  WHERE status IN ('pending', 'running');

CREATE INDEX idx_ariregister_source_translation_queue_pending
  ON ariregister_source.translation_queue_entries(status, status_changed_at, company_id)
  WHERE status = 'pending';

CREATE INDEX idx_ariregister_source_translation_queue_running
  ON ariregister_source.translation_queue_entries(status, status_changed_at)
  WHERE status = 'running';

CREATE INDEX idx_ariregister_source_translation_queue_batch
  ON ariregister_source.translation_queue_entries(batch_id)
  WHERE batch_id IS NOT NULL;
