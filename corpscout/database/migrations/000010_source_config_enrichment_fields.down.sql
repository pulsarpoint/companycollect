-- Restore original fields lists from migration 000004/000005.

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "lei", "status"]}'::jsonb
WHERE name = 'gleif';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "lei"]}'::jsonb
WHERE name = 'opencorporates';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status"]}'::jsonb
WHERE name = 'companies_house';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "website"]}'::jsonb
WHERE name = 'brreg';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status", "website"]}'::jsonb
WHERE name = 'cvr';

UPDATE data_sources SET config = config || '{"fields": ["name", "country", "registration_number", "status"]}'::jsonb
WHERE name = 'ariregister';
