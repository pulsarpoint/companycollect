UPDATE data_sources SET config = '{}'::jsonb
WHERE name IN ('gleif','wikidata','opencorporates','companies_house','brreg','cvr','ariregister');
