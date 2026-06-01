UPDATE data_sources SET config = config || '{
  "notes": "UK Companies House official registry. API key required (HTTP Basic auth, key as username, empty password). Without key returns empty results."
}'::jsonb WHERE name = 'companies_house';

UPDATE data_sources SET config = config || '{
  "notes": "Danish Central Business Register. API token required. API does not return a total record count."
}'::jsonb WHERE name = 'cvr';
