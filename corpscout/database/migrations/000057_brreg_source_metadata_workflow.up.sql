UPDATE data_sources
SET input_table_name = 'brreg_workflow.raw_records',
    pull_task_type = 'brreg_bulk_ingest',
    processor_task_type = NULL,
    updated_at = now()
WHERE name = 'brreg';
