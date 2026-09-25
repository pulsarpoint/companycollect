DROP TABLE processing.llm_external_requests;
DROP TABLE processing.run_llm_dependencies;
DROP TABLE processing.run_requests;
DROP TABLE processing.llm_catalog_imports;
DROP TABLE processing.llm_checks;
ALTER TABLE processing.llm_profiles DROP CONSTRAINT llm_current_revision;
DROP TABLE processing.llm_profile_revisions;
DROP FUNCTION processing.protect_llm_revision();
DROP TABLE processing.llm_profiles;
