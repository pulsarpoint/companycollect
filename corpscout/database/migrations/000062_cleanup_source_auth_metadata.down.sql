UPDATE data_sources
SET
  config = jsonb_set(config, '{auth_env}', '"CRAWLER_OPENCORPORATES_API_KEY"'::jsonb),
  updated_at = now()
WHERE name = 'opencorporates'
  AND config->>'auth_env' = 'OPENCORPORATES_API_KEY';
