ALTER TABLE company_financials
    DROP CONSTRAINT IF EXISTS chk_company_financials_evidence_object,
    DROP COLUMN IF EXISTS evidence;
