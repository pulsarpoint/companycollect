ALTER TABLE company_financials
    ADD COLUMN IF NOT EXISTS evidence JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE company_financials
    DROP CONSTRAINT IF EXISTS chk_company_financials_evidence_object,
    ADD CONSTRAINT chk_company_financials_evidence_object
        CHECK (jsonb_typeof(evidence) = 'object');
