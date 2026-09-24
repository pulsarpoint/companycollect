DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'processing_worker') THEN
        REVOKE SELECT, INSERT, UPDATE ON processing.input_submissions FROM processing_worker;
    END IF;
END $$;
