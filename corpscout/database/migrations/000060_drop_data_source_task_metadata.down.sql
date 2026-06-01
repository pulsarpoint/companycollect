ALTER TABLE data_sources
  ADD COLUMN pull_task_type TEXT NOT NULL DEFAULT 'unregistered',
  ADD COLUMN processor_task_type TEXT;

UPDATE data_sources
SET pull_task_type = 'temporal_download'
WHERE name IN ('companies_house', 'gleif', 'cvr', 'ariregister');

UPDATE data_sources
SET pull_task_type = 'brreg_bulk_ingest'
WHERE name = 'brreg';
