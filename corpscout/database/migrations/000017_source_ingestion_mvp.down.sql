DROP TABLE IF EXISTS source_processor_states;
DROP TABLE IF EXISTS source_pull_runs;

ALTER TABLE companies DROP CONSTRAINT IF EXISTS companies_primary_source_id_fkey;
ALTER TABLE company_aliases DROP CONSTRAINT IF EXISTS company_aliases_source_id_fkey;

DROP TABLE IF EXISTS data_sources;
