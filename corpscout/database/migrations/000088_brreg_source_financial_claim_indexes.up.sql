CREATE INDEX IF NOT EXISTS idx_brreg_company_process_status_financial_running_lease
  ON brreg_source.company_process_status(financial_lease_until, company_id)
  WHERE financial_status = 'running';

CREATE INDEX IF NOT EXISTS idx_brreg_company_process_status_financial_ready_order
  ON brreg_source.company_process_status(updated_at, company_id)
  INCLUDE (financial_status, financial_attempt_count)
  WHERE financial_status IN ('pending', 'dirty', 'failed_retryable');
