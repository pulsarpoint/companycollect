DROP TRIGGER IF EXISTS trg_brreg_source_companies_process_status ON brreg_source.companies;
DROP FUNCTION IF EXISTS brreg_source.ensure_company_process_status();
