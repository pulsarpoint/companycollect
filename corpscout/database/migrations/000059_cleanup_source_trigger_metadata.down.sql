UPDATE data_sources
SET pull_task_type = 'source_pull',
    updated_at = now()
WHERE name = 'companies_house';

UPDATE data_sources
SET pull_task_type = 'data_task',
    updated_at = now()
WHERE name IN ('gleif', 'cvr', 'ariregister');

UPDATE data_sources
SET pull_task_type = 'brreg_bulk_ingest',
    schedule_kind = 'interval',
    schedule_expression = '24h',
    updated_at = now()
WHERE name = 'brreg';

UPDATE data_sources
SET pull_task_type = 'source_pull',
    updated_at = now()
WHERE name IN ('wikidata', 'opencorporates');
