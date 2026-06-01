UPDATE data_sources
SET processor_task_type = 'source_process',
    updated_at = now()
WHERE name IN (
    'gleif',
    'companies_house',
    'brreg',
    'ai_company_profile',
    'cvr',
    'ariregister',
    'opencorporates'
);
