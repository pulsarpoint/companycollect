-- Seed known data sources. Sources without a country are global (country_id NULL).
INSERT INTO data_sources (name, source_type, adapter_type, country_id, enabled, crawl_interval_hours, config)
VALUES
    ('gleif',          'global_aggregator',  'api', NULL,                                                     true,  24,  '{}'),
    ('wikidata',       'global_aggregator',  'api', NULL,                                                     true,  168, '{}'),
    ('opencorporates', 'global_aggregator',  'api', NULL,                                                     false, 72,  '{}'),
    ('companies_house','country_registry',   'api', (SELECT id FROM countries WHERE iso_alpha2 = 'GB'),       true,  24,  '{}'),
    ('brreg',          'country_registry',   'api', (SELECT id FROM countries WHERE iso_alpha2 = 'NO'),       true,  24,  '{}'),
    ('cvr',            'country_registry',   'api', (SELECT id FROM countries WHERE iso_alpha2 = 'DK'),       true,  24,  '{}'),
    ('ariregister',    'country_registry',   'api', (SELECT id FROM countries WHERE iso_alpha2 = 'EE'),       false, 24,  '{}')
ON CONFLICT (name) DO NOTHING;
