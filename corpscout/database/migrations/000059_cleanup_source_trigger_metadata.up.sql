UPDATE data_sources
SET pull_task_type = 'temporal_download',
    processor_task_type = NULL,
    updated_at = now()
WHERE name IN ('companies_house', 'gleif', 'cvr', 'ariregister');

UPDATE data_sources
SET input_table_name = 'brreg_workflow.raw_records',
    pull_task_type = 'brreg_bulk_ingest',
    processor_task_type = NULL,
    enabled = false,
    schedule_enabled = false,
    schedule_kind = 'manual',
    schedule_expression = NULL,
    updated_at = now()
WHERE name = 'brreg';

UPDATE data_sources
SET pull_task_type = 'unregistered',
    processor_task_type = NULL,
    enabled = false,
    schedule_enabled = false,
    schedule_kind = 'manual',
    schedule_expression = NULL,
    updated_at = now()
WHERE name IN ('wikidata', 'opencorporates');
