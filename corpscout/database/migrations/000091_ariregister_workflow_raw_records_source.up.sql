UPDATE data_sources
SET input_table_name = 'ariregister_workflow.raw_records',
    updated_at = now()
WHERE name = 'ariregister';
