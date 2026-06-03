CREATE TABLE brreg_source.company_process_status (
  company_id UUID PRIMARY KEY REFERENCES brreg_source.companies(id) ON DELETE CASCADE,

  translation_status TEXT NOT NULL DEFAULT 'pending',
  translation_attempt_count INTEGER NOT NULL DEFAULT 0,
  translation_lease_by TEXT,
  translation_lease_until TIMESTAMPTZ,
  translation_last_started_at TIMESTAMPTZ,
  translation_last_finished_at TIMESTAMPTZ,
  translation_error TEXT,
  translation_error_category TEXT,
  translation_error_code TEXT,
  translation_retry_strategy TEXT,
  translation_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  currency_status TEXT NOT NULL DEFAULT 'pending',
  currency_attempt_count INTEGER NOT NULL DEFAULT 0,
  currency_lease_by TEXT,
  currency_lease_until TIMESTAMPTZ,
  currency_last_started_at TIMESTAMPTZ,
  currency_last_finished_at TIMESTAMPTZ,
  currency_error TEXT,
  currency_error_category TEXT,
  currency_error_code TEXT,
  currency_retry_strategy TEXT,
  currency_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  financial_status TEXT NOT NULL DEFAULT 'pending',
  financial_attempt_count INTEGER NOT NULL DEFAULT 0,
  financial_lease_by TEXT,
  financial_lease_until TIMESTAMPTZ,
  financial_last_started_at TIMESTAMPTZ,
  financial_last_finished_at TIMESTAMPTZ,
  financial_error TEXT,
  financial_error_category TEXT,
  financial_error_code TEXT,
  financial_retry_strategy TEXT,
  financial_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_company_process_status_translation_status CHECK (
    translation_status IN ('pending', 'dirty', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_brreg_company_process_status_currency_status CHECK (
    currency_status IN ('pending', 'dirty', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_brreg_company_process_status_financial_status CHECK (
    financial_status IN ('pending', 'dirty', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_brreg_company_process_status_translation_attempt_count CHECK (translation_attempt_count >= 0),
  CONSTRAINT chk_brreg_company_process_status_currency_attempt_count CHECK (currency_attempt_count >= 0),
  CONSTRAINT chk_brreg_company_process_status_financial_attempt_count CHECK (financial_attempt_count >= 0),
  CONSTRAINT chk_brreg_company_process_status_translation_metadata_object CHECK (jsonb_typeof(translation_metadata) = 'object'),
  CONSTRAINT chk_brreg_company_process_status_currency_metadata_object CHECK (jsonb_typeof(currency_metadata) = 'object'),
  CONSTRAINT chk_brreg_company_process_status_financial_metadata_object CHECK (jsonb_typeof(financial_metadata) = 'object')
);

CREATE INDEX idx_brreg_company_process_status_translation_queue
  ON brreg_source.company_process_status(translation_status, translation_lease_until, updated_at, company_id)
  WHERE translation_status IN ('pending', 'dirty', 'running', 'failed_retryable');

CREATE INDEX idx_brreg_company_process_status_currency_queue
  ON brreg_source.company_process_status(currency_status, currency_lease_until, updated_at, company_id)
  WHERE currency_status IN ('pending', 'dirty', 'running', 'failed_retryable');

CREATE INDEX idx_brreg_company_process_status_financial_queue
  ON brreg_source.company_process_status(financial_status, financial_lease_until, updated_at, company_id)
  WHERE financial_status IN ('pending', 'dirty', 'running', 'failed_retryable');

INSERT INTO brreg_source.company_process_status (company_id)
SELECT company.id
FROM brreg_source.companies company
WHERE company.row_status = 'active'
ON CONFLICT (company_id) DO NOTHING;
