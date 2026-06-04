UPDATE data_sources
SET input_table_name = 'ariregister_company_raw_inputs',
    updated_at = now()
WHERE name = 'ariregister';
