UPDATE data_sources
SET input_table_name = 'brreg_company_raw_inputs',
    pull_task_type = 'source_pull',
    processor_task_type = NULL,
    updated_at = now()
WHERE name = 'brreg';
