CREATE OR REPLACE FUNCTION brreg_source.ensure_company_process_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO brreg_source.company_process_status (company_id)
  VALUES (NEW.id)
  ON CONFLICT (company_id) DO NOTHING;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_brreg_source_companies_process_status ON brreg_source.companies;

CREATE TRIGGER trg_brreg_source_companies_process_status
AFTER INSERT OR UPDATE OF row_status ON brreg_source.companies
FOR EACH ROW
WHEN (NEW.row_status = 'active')
EXECUTE FUNCTION brreg_source.ensure_company_process_status();

INSERT INTO brreg_source.company_process_status (
  company_id,
  financial_status,
  financial_last_finished_at
)
SELECT
  company.id,
  CASE
    WHEN financial_company.company_id IS NOT NULL THEN 'succeeded'
    ELSE 'pending'
  END AS financial_status,
  CASE
    WHEN financial_company.company_id IS NOT NULL THEN now()
    ELSE NULL
  END AS financial_last_finished_at
FROM brreg_source.companies company
LEFT JOIN (
  SELECT DISTINCT financial_statement.company_id
  FROM brreg_source.financial_statements financial_statement
) financial_company ON financial_company.company_id = company.id
WHERE company.row_status = 'active'
ON CONFLICT (company_id) DO NOTHING;

UPDATE brreg_source.company_process_status status_row
SET
  financial_status = 'succeeded',
  financial_lease_by = NULL,
  financial_lease_until = NULL,
  financial_last_finished_at = coalesce(status_row.financial_last_finished_at, now()),
  financial_error = NULL,
  financial_error_category = NULL,
  financial_error_code = NULL,
  financial_retry_strategy = NULL,
  updated_at = now()
WHERE status_row.financial_status IN ('pending', 'dirty', 'running', 'failed_retryable')
  AND EXISTS (
    SELECT 1
    FROM brreg_source.financial_statements financial_statement
    WHERE financial_statement.company_id = status_row.company_id
  );
