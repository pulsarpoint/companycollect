-- The worker was provisioned before migration 124 created input_submissions.
-- Existing grants on ALL TABLES do not include subsequently created tables.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'processing_worker') THEN
        GRANT SELECT, INSERT, UPDATE ON processing.input_submissions TO processing_worker;
    END IF;
END $$;
