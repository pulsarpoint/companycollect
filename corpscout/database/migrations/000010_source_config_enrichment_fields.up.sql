-- Update source config fields lists to reflect enrichment data now extracted
-- from each adapter's raw API response.

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "lei", "status", "address", "hq_address", "aliases"]}'::jsonb
WHERE name = 'gleif';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "lei", "address", "founded_year", "industries"]}'::jsonb
WHERE name = 'opencorporates';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "website", "address", "founded_year", "industries"]}'::jsonb
WHERE name = 'companies_house';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "website", "address", "industries", "founded_year", "employees"]}'::jsonb
WHERE name = 'brreg';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "website", "address", "phone", "email", "industries"]}'::jsonb
WHERE name = 'cvr';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "address", "industries"]}'::jsonb
WHERE name = 'ariregister';
