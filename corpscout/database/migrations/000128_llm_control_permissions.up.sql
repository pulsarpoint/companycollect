DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='processing_worker') THEN
        GRANT USAGE ON SCHEMA processing TO processing_worker;
        GRANT SELECT,INSERT,UPDATE ON processing.llm_profiles,processing.llm_profile_revisions,
            processing.llm_checks,processing.llm_catalog_imports,processing.run_requests,
            processing.run_llm_dependencies,processing.llm_external_requests TO processing_worker;
    END IF;
END $$;
